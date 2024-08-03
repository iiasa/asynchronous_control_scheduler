import io
import os
import time
import uuid
import json
import base64
import traceback
import asyncio
import threading
from queue import Queue, Empty as QueueEmptyError
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout, redirect_stderr
from celery import Celery
from celery.signals import worker_ready
from celery.exceptions import SoftTimeLimitExceeded

from accli import AjobCliService
from acc_native_jobs.merge_csv_regional_timeseries import CSVRegionalTimeseriesMergeService

from acc_native_jobs.validate_csv_regional_timeseries import CsvRegionalTimeseriesVerificationService
from k8_gateway_actions.dispatch_build_and_push import DispachWkubeTask
from .IamcVerificationService import IamcVerificationService
from .exceptions import WkubeRetryException
from k8_gateway_actions.registries import create_default_registry_secret_resource
from k8_gateway_actions.service_accounts import add_pvc_role_to_service_account
from k8_gateway_actions.cleanup_tasks import delete_pvc, delete_orphan_pvcs

from acc_native_jobs import celeryconfig
from configs.Environment import get_environment_variables

from kubernetes import client, config, dynamic
from kubernetes.client import api_client

env = get_environment_variables()


app = Celery('acc_native_jobs')

app.config_from_object(celeryconfig)


class RemoteStreamWriter(io.TextIOBase):
    def __init__(self, project_service: AjobCliService, chunk_size=124, max_workers=20):
        super().__init__()
        self.project_service = project_service
        self.buffer = ''
        self.chunk_size = chunk_size
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.log_counter = int(time.time())
        self.verbose_unhealthy_report = Queue()
        self.job_is_unhealthy = threading.Event()
        self.stop_event = threading.Event()
        self.lock = threading.RLock()
        self.write_thread = threading.Thread(target=self._periodic_write)
        self.write_thread.start()

    def write(self, data):
        if self.job_is_unhealthy.is_set():
            self.stop_event.set()
            self.executor.shutdown(wait=False, cancel_futures=True)
            
            try:
                verbose_error = self.verbose_unhealthy_report.get(timeout=10)
            except QueueEmptyError:
                verbose_error = "Cannot get error"
            raise ValueError(verbose_error)
        
        with self.lock:
            self.buffer += data

    def _periodic_write(self):
        while not self.stop_event.is_set():
            print('is it ticking')
            if self.job_is_unhealthy.is_set():
                # Line below should trigger write function if there is nothing else from actual function to write.
                print("Terminating job because of health issue.")
            else:
                time.sleep(10)
                if not self.job_is_unhealthy.is_set():
                    self.flush()

    def flush(self):
        with self.lock:
            if self.buffer:
                chunk = self.buffer
                self.buffer = ''
                filename = f"celery{self.log_counter}.log"
                self.log_counter += 1
                self.executor.submit(self._send_chunk, chunk, filename)
            else:
                self.executor.submit(self.check_job_health)
                

    def check_job_health(self):
        is_healthy = self.project_service.check_job_health()
        if not is_healthy:
            self.verbose_unhealthy_report.put("Job is not healthy anymore.")
            self.job_is_unhealthy.set()
            print("Set via health check")

        print(f"Printing is_healthy {is_healthy}")
                
    def _send_chunk(self, chunk, filename):
        self._send_request(chunk, filename)
        
    def _send_request(self, chunk, filename):
        try:
            self.project_service.add_log_file(
                    chunk.encode(),
                    filename,
                )
        except Exception as err:
            self.verbose_unhealthy_report.put(f"Unhealthy reason: {str(err)}")
            self.job_is_unhealthy.set()
            print("Set via log")
        
        print(f"Printing IS HEALTHY")
        

    def close(self):
        self.last_close = True
        self.stop_event.set()
        self.write_thread.join()
        self.flush()
        self.executor.shutdown(wait=True)        

def capture_log(func):
    """Capture stdout and stderr to accelerator data repo"""

    def wrapper_func(*args, **kwargs):
        job_token = kwargs['job_token']    
        project_service = AjobCliService(
            job_token,
            server_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )

        project_service.update_job_status("PROCESSING")

        log_stream = RemoteStreamWriter(project_service, chunk_size=1024)

        error = None

        with redirect_stdout(log_stream), redirect_stderr(log_stream):
            try:
                func(*args, **kwargs)

            except Exception as err:
                
                error = err
                error_message = ''.join(traceback.format_exc())
                log_stream.write(error_message)

        
        log_stream.close()

        if not error:
            project_service.update_job_status("DONE")
        else:
            project_service.update_job_status("ERROR")
            raise error
   
        
    return wrapper_func

def wkube_capture_log(func):
    """Capture stdout and stderr to accelerator data repo"""

    def wrapper_func(*args, **kwargs):
        
        job_token = kwargs['job_token']    
        
        project_service = AjobCliService(
            job_token,
            server_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )

        project_service.update_job_status("PREPARING")

        log_stream = RemoteStreamWriter(project_service, chunk_size=1024)

        error = None

        with redirect_stdout(log_stream), redirect_stderr(log_stream):
            try:
                func(*args, **kwargs)
                
            except Exception as err:
                error = err
                error_message = ''.join(traceback.format_exc())
                log_stream.write(error_message)

        log_stream.close()

        if error:
            project_service.update_job_status("ERROR")
            raise error
        
        
    return wrapper_func

def handle_soft_time_limit(func):
    def wrapper_func(*args, **kwargs):
        
        job_token = kwargs['job_token']    
        
        project_service = AjobCliService(
            job_token,
            server_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )
        try:
            func(*args, **kwargs)
        except SoftTimeLimitExceeded:
            print("Job timeout")
            
    return wrapper_func


def handle_wkube_soft_time_limit(func):
    
    def wrapper_func(*args, **kwargs):
        
        job_token = kwargs['job_token']    
        
        project_service = AjobCliService(
            job_token,
            server_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )
        try:
            func(*args, **kwargs)
        except SoftTimeLimitExceeded:
            print("Building timeout")

    return wrapper_func

@worker_ready.connect
def at_start(sender, **k):

    if not env.WKUBE_SECRET_JSON_B64:
        return

    create_default_registry_secret_resource()

    print("Default registries created.")


@app.task(
        name='acc_native_jobs.clean_unused_pvcs_task'
    )
def clean_unused_pvcs_task():

    if not env.WKUBE_SECRET_JSON_B64:
        print("Wkube k8s secrests env var WKUBE_SECRET_JSON_B64 not set.")
        return

    delete_orphan_pvcs()
    print('Unused pvcs cleaned')


@app.task(
        name='acc_native_jobs.delete_pvc_task'
    )
def delete_pvc_task(pvc_name):

    if not env.WKUBE_SECRET_JSON_B64:
        print("Wkube k8s secrests env var WKUBE_SECRET_JSON_B64 not set.")
        return

    delete_pvc(pvc_name)
    print(f'Pvc deleted: {pvc_name}')
        
@app.task(
        name='acc_native_jobs.verify_csv_regional_timeseries'
    )
@capture_log
@handle_soft_time_limit
def verify_csv_regional_timeseries(*args, **kwargs):
    csv_regional_timeseries_verification_service = CsvRegionalTimeseriesVerificationService(*args, **kwargs)
    csv_regional_timeseries_verification_service()

@app.task(
        name='acc_native_jobs.merge_csv_regional_timeseries'
    )
@capture_log
@handle_soft_time_limit
def merge_csv_regional_timeseries(*args, **kwargs):
    csv_regional_timeseries_merge_service = CSVRegionalTimeseriesMergeService(*args, **kwargs)
    csv_regional_timeseries_merge_service()


@app.task(
        name='acc_native_jobs.dispatch_wkube_task'
    )
@wkube_capture_log
@handle_wkube_soft_time_limit
def dispatch_wkube_task(*args, **kwargs):
    dispatch = DispachWkubeTask(*args, **kwargs)
    dispatch()