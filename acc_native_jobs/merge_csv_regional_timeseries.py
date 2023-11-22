import io
import os
import json
import uuid
import pyam
from typing import Callable, TypedDict, Iterator
from accli import ACliService
from dateutil.parser import parse as parse_date
from configs.Environment import get_environment_variables

env = get_environment_variables()


def iterable_to_stream(iterable, buffer_size=io.DEFAULT_BUFFER_SIZE):
    """
    Lets you use an iterable (e.g. a generator) that yields bytestrings as a read-only
    input stream.

    The stream implements Python 3's newer I/O API (available in Python 2's io module).
    For efficiency, the stream is buffered.
    """

    class IterStream(io.RawIOBase):
        def __init__(self):
            self.leftover = None

        def readable(self):
            return True

        def readinto(self, b):
            try:
                l = len(b)  # We're supposed to return at most this much
                # TODO @wrufesh find why the below line does not raise generator exception
                chunk = self.leftover or next(iterable)
                output, self.leftover = chunk[:l], chunk[l:]
                b[: len(output)] = output
                return len(output)
            except StopIteration:
                return 0  # indicate EOF

    return io.BufferedReader(IterStream(), buffer_size=buffer_size)



class CSVRegionalTimeseriesMergeService:
    def __init__(
        self,
        *,
        bucket_object_id_list: list[int],
        job_token
    ):

        self.temp_dir = f"tmp_files"

        self.project_service = ACliService(
            job_token,
            cli_base_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )


        # with remote job user will be denoted by token

        self.bucket_object_id_list = bucket_object_id_list


    
    def check_input_files_has_same_type(self):
        for bucket_object_id in self.bucket_object_id_list:
            type_name = self.project_service.get_bucket_object_validation_type(
                bucket_object_id
            )
            if type_name != "IAMC":
                return False
        return True       
           

    
    def delete_local_file(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)


    def __call__(self):
        self.check_individual_file_iamc_validation()

        