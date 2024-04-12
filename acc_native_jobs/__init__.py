import io
import os
import time
import uuid
import traceback
import asyncio
import threading
from contextlib import redirect_stdout, redirect_stderr
from celery import Celery

from accli import AjobCliService
from acc_native_jobs.merge_csv_regional_timeseries import CSVRegionalTimeseriesMergeService

from acc_native_jobs.validate_csv_regional_timeseries import CsvRegionalTimeseriesVerificationService
from k8_gateway_actions.dispatch_build_and_push import DispachWkubeTask
from .IamcVerificationService import IamcVerificationService

from acc_native_jobs import celeryconfig
from configs.Environment import get_environment_variables

env = get_environment_variables()


app = Celery('acc_native_jobs')

app.config_from_object(celeryconfig)


class RemoteStreamWriter(io.TextIOBase):
    def __init__(self, project_service: AjobCliService, chunk_size=124):
        super().__init__()
        self.project_service = project_service
        self.buffer = ''
        self.chunk_size = chunk_size
        self.threads = []
        self.log_counter = 0
       

    def write(self, data):
        self.buffer += data
        while len(self.buffer) >= self.chunk_size:
            chunk, self.buffer = self.buffer[:self.chunk_size], self.buffer[self.chunk_size:]
            filename = f"celery{self.log_counter}.log"
            self.log_counter += 1
            t = threading.Thread(target=self._send_chunk, args=(chunk, filename))
            t.start()
            self.threads.append(t)

    def flush(self):
        if self.buffer:
            chunk = self.buffer[:]
            filename = f"celery{self.log_counter}.log"
            self.log_counter += 1
            t = threading.Thread(target=self._send_chunk, args=(chunk, filename))
            t.start()
            self.threads.append(t)
            self.buffer = ''

    def _send_chunk(self, chunk, filename):
        # print(f"Sending {filename}")
        self._send_request(chunk, filename)
        # print(f"{filename} sent")
        
    def _send_request(self, chunk, filename):
        self.project_service.add_log_file(
                chunk.encode(),
                filename,
                
            )

    def close(self):
        self.flush()
        for thread in self.threads:
            thread.join()
        

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



@app.task(name='verify_csv_regional_timeseries')
@capture_log
def verify_csv_regional_timeseries(*args, **kwargs):
    csv_regional_timeseries_verification_service = CsvRegionalTimeseriesVerificationService(*args, **kwargs)
    csv_regional_timeseries_verification_service()

@app.task(name='merge_csv_regional_timeseries')
@capture_log
def merge_csv_regional_timeseries(*args, **kwargs):
    csv_regional_timeseries_merge_service = CSVRegionalTimeseriesMergeService(*args, **kwargs)
    csv_regional_timeseries_merge_service()



@app.task(name='dispatch_wkube_task')
@wkube_capture_log
def dispatch_wkube_task(*args, **kwargs):
    print("builder")
    dispatch = DispachWkubeTask(*args, **kwargs)
    dispatch()
    