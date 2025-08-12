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
from celery.exceptions import SoftTimeLimitExceeded, Retry as CeleryRetry

from accli import AjobCliService
from acc_worker.acc_native_jobs.merge_csv_regional_timeseries import CSVRegionalTimeseriesMergeService

from acc_worker.acc_native_jobs.validate_csv_regional_timeseries import CsvRegionalTimeseriesVerificationService
from acc_worker.k8_gateway_actions.dispatch_build_and_push import DispachWkubeTask
from .exceptions import WkubeRetryException
from acc_worker.k8_gateway_actions.registries import create_default_registry_secret_resource
from acc_worker.k8_gateway_actions.service_accounts import add_pvc_role_to_service_account
from acc_worker.k8_gateway_actions.periodic_tasks import delete_pvc, delete_orphan_pvcs, update_stalled_jobs_status

from acc_worker.acc_native_jobs import celeryconfig
from acc_worker.configs.Environment import get_environment_variables

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
        self.job_is_unhealthy = threading.Event()
        self.stop_event = threading.Event()
        self.lock = threading.RLock()
        self.write_thread = threading.Thread(target=self._periodic_write)
        self.write_thread.start()
        self.flush_in_progress = False

    def write(self, data):
        if self.job_is_unhealthy.is_set():
            self.stop_event.set()
            with self.lock:
                self.buffer += data
                self.buffer += '\n **** Job is not healthy anymore **** \n'
            self.final_flush()
            self.executor.shutdown(wait=False, cancel_futures=True)
            raise ValueError("Job health reported to be bad.")
        with self.lock:
            self.buffer += data

    def _periodic_write(self):
        while not self.stop_event.is_set():
            self.stop_event.wait(10)
            if not self.job_is_unhealthy.is_set():
                self.flush()


    def final_flush(self):
        with self.lock:
            chunk = self.buffer
            self.buffer = ''
            filename = f"celery{self.log_counter}.log"
            self.log_counter += 1

        if chunk:
            try:
                self._send_chunk(chunk, filename)  # Synchronous flush
            except Exception as e:
                print(f"[RemoteStreamWriter] Final flush failed: {e}")
    
    def flush(self):
        with self.lock:
            if self.flush_in_progress:
                return

            if not self.buffer:
                self.flush_in_progress = True

                def _health_check():
                    try:
                        self.check_job_health()
                    finally:
                        with self.lock:
                            self.flush_in_progress = False

                self.executor.submit(_health_check)
                return

            chunk = self.buffer
            self.buffer = ''
            filename = f"celery{self.log_counter}.log"
            self.log_counter += 1
            self.flush_in_progress = True

        def _flush_task():
            try:
                self._send_chunk(chunk, filename)
            finally:
                with self.lock:
                    self.flush_in_progress = False

        self.executor.submit(_flush_task)
                    

    def check_job_health(self):
        is_healthy = self.project_service.check_job_health()
        if not is_healthy:
            self.job_is_unhealthy.set()

                
    def _send_chunk(self, chunk, filename):
        self._send_request(chunk, filename)
        
    def _send_request(self, chunk, filename):
        is_healthy = self.project_service.add_log_file(
                chunk.encode(),
                filename,
            )
        if not is_healthy:
            self.job_is_unhealthy.set()
        
    def close(self):
        self.last_close = True
        self.stop_event.set()
        self.write_thread.join()
        self.final_flush()
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
            if not isinstance(error, CeleryRetry):
                project_service.update_job_status("ERROR")
            raise error
        else:
            if kwargs['build_only_task']:
                project_service.update_job_status("DONE")
        
        
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
        name='acc_native_jobs.update_stalled_jobs_status'
    )
def clean_unused_pvcs_task():

    update_stalled_jobs_status()
    print('Stalled jobs status updated')


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

    selected_files_ids = kwargs['selected_files_ids']
    selected_filenames = kwargs['selected_filenames']
    dataset_template_id = kwargs['dataset_template_id']

    for index in range(len(selected_files_ids)):
        filename = selected_filenames[index]
        bucket_object_id = selected_files_ids[index]
        
        print(f"_____________Validating file: {filename} _____________")
   
        csv_regional_timeseries_verification_service = CsvRegionalTimeseriesVerificationService(
            bucket_object_id=bucket_object_id,
            dataset_template_id=dataset_template_id,
            job_token=kwargs.get('job_token'),
            s3_filename=filename
        )
        csv_regional_timeseries_verification_service()

        print(f"_____________DONE: Validating file: {filename} _____________")

@app.task(
        name='acc_native_jobs.merge_csv_regional_timeseries'
    )
@capture_log
@handle_soft_time_limit
def merge_csv_regional_timeseries(*args, **kwargs):
    selected_filenames = kwargs['selected_filenames']

    print(f"_____________Merging following files: {selected_filenames} _____________")
    
    merged_filename = kwargs.get('merged_filename')
    bucket_object_id_list = kwargs['selected_files_ids']
    csv_regional_timeseries_merge_service = CSVRegionalTimeseriesMergeService(
        filename=merged_filename,
        bucket_object_id_list=bucket_object_id_list,
        job_token=kwargs.get('job_token')
    )
    csv_regional_timeseries_merge_service()


@app.task(
        name='acc_native_jobs.dispatch_wkube_task'
    )
@wkube_capture_log
@handle_wkube_soft_time_limit
def dispatch_wkube_task(*args, **kwargs):
    dispatch = DispachWkubeTask(*args, **kwargs)
    dispatch()