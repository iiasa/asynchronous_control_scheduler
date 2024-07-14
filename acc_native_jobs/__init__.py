import io
import os
import time
import uuid
import traceback
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout, redirect_stderr
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded

from accli import AjobCliService
from acc_native_jobs.merge_csv_regional_timeseries import CSVRegionalTimeseriesMergeService

from acc_native_jobs.validate_csv_regional_timeseries import CsvRegionalTimeseriesVerificationService
from k8_gateway_actions.dispatch_build_and_push import DispachWkubeTask
from .IamcVerificationService import IamcVerificationService
from .exceptions import WkubeRetryException

from acc_native_jobs import celeryconfig
from configs.Environment import get_environment_variables

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
        self.log_counter = 0
        self.stop_event = threading.Event()
        self.lock = threading.RLock()
        self.write_thread = threading.Thread(target=self._periodic_write)
        self.write_thread.start()

    def write(self, data):
        with self.lock:
            self.buffer += data

    def _periodic_write(self):
        while not self.stop_event.is_set():
            time.sleep(10)
            self.flush()

    def flush(self):
        with self.lock:
            if self.buffer:
                chunk = self.buffer
                self.buffer = ''
                filename = f"celery{self.log_counter}.log"
                self.log_counter += 1
                self.executor.submit(self._send_chunk, chunk, filename)
                

    def _send_chunk(self, chunk, filename):
        self._send_request(chunk, filename)
        
    def _send_request(self, chunk, filename):
        try:
            self.project_service.add_log_file(
                    chunk.encode(),
                    filename,
                )
        except Exception as err:
            print(str(err))
            os._exit(1)

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

        has_error = False

        with redirect_stdout(log_stream), redirect_stderr(log_stream):
            try:
                func(*args, **kwargs)
                has_error = False
                
            except Exception as err:
                
                has_error = True
                error_message = ''.join(traceback.format_exc())
                log_stream.write(error_message)

        
        log_stream.close()

        if not has_error:
            project_service.update_job_status("DONE")
        else:
            project_service.update_job_status("ERROR")
   
        
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

        has_error = False

        with redirect_stdout(log_stream), redirect_stderr(log_stream):
            try:
                func(*args, **kwargs)
                
            except Exception as err:
                has_error = True
                error_message = ''.join(traceback.format_exc())
                log_stream.write(error_message)

        log_stream.close()

        if has_error:
            project_service.update_job_status("ERROR")
        
        
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
        name='acc_native_jobs.dispatch_wkube_task',
        autoretry_for = (WkubeRetryException,),
    )
@wkube_capture_log
@handle_wkube_soft_time_limit
def dispatch_wkube_task(*args, **kwargs):
    dispatch = DispachWkubeTask(*args, **kwargs)
    dispatch()