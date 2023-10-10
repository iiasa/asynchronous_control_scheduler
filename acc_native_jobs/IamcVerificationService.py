import os
import json
import uuid
import logging
import pyam
from typing import Callable, TypedDict, Iterator
from configs.Environment import get_environment_variables

from accli import ACliService

env = get_environment_variables()

class ReleaseError(ValueError):
    pass

class CreatedObject(TypedDict):
    model: str
    pkey: str | int


class MoreDiskMoreRamHandler:
    def __init__(self, verify_service_instance: "IamcVerificationService"):
        self.temp_dir = f"background_tasks/temp"
        self.project_service = verify_service_instance.project_service
        self.bucket_object_id = verify_service_instance.bucket_object_id
        self.temp_downloaded_filename = f"{uuid.uuid4().hex}.csv"
        self.temp_converted_wide_filename = f"{uuid.uuid4().hex}.csv"

        self.temp_downloaded_filepath = (
            f"{self.temp_dir}/{self.temp_downloaded_filename}"
        )
        self.temp_converted_wide_filepath = (
            f"{self.temp_dir}/{self.temp_converted_wide_filename}"
        )

        self.df = None
        self.is_long_format = False

        self.meta_db = None

        self()

    def __call__(self):
        # TODO @wrufesh find if the input file in long or wide
        # TODO @wrufesh if already wide no need to upload again

        try:
            self.download_file()

            self.validate_file()

            self.delete_local_file(self.temp_downloaded_filepath)

            self.generate_meta_db()

            self.generate_wide_csv()

            self.save_generated_files()

        finally:
            self.delete_local_file(self.temp_downloaded_filepath)
            self.delete_local_file(f"{self.temp_dir}/{self.meta_data['filename']}")
            self.delete_local_file(self.temp_converted_wide_filepath)

    def download_file(self):
        response = self.project_service.get_file_stream(
            self.bucket_object_id
        )

        with open(self.temp_downloaded_filepath, "wb") as tmp_file:
            for data in response.stream(amt=1024 * 1024):
                size = tmp_file.write(data)

        response.release_conn()

    def validate_file(self):
        self.df = pyam.IamDataFrame(data=self.temp_downloaded_filepath)

    def delete_local_file(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    def generate_meta_db(self):
        #store in s3 in such a way that you can make use of scan range
        #for pagination

        self.meta_data = dict(
            filename=f"{uuid.uuid4().hex}.json",
            data=[]
        )

        headers = self.df.data.columns.values.tolist()

        self.meta_data['data'] += [dict(table='csv_headers', name=header) for header in headers]

        for header in headers:
            if header not in [
                "value",
                # "unit"
            ]:
                values = getattr(self.df, header)

                self.meta_data['data'] += [dict(table=header, name=value) for value in values]

        with open(f"{self.temp_dir}/{self.meta_data['filename']}", 'w') as fp:
            for row in self.meta_data['data']:
                json.dump(row, fp)
                fp.write('\n')
                

    def generate_wide_csv(self):
        self.df.to_csv(self.temp_converted_wide_filepath)

    def save_generated_files(self):
        db_index_bucket_object_id = self.upload_new_file(
            f"{self.temp_dir}/{self.meta_data['filename']}", self.meta_data['filename']
        )
        self.replace_file_content(
            self.temp_converted_wide_filepath, self.bucket_object_id
        )

        self.project_service.register_iamc_validation(
            self.bucket_object_id, db_index_bucket_object_id
        )

    def upload_new_file(self, local_file_path, bucket_filename):
        with open(local_file_path, "rb") as file_stream:
            bucket_object_id = self.project_service.add_filestream_as_job_output(
                bucket_filename,
                file_stream
            )

        return bucket_object_id

    def replace_file_content(self, local_file_path, bucket_object_id):
        with open(local_file_path, "rb") as file_stream:
            bucket_object_id = self.project_service.replace_bucket_object_id_content(
                bucket_object_id,
                file_stream,
            )

        return bucket_object_id


class IamcVerificationService:
    def __init__(
        self,
        *,
        bucket_object_id,
        job_token,
        ram_required=4 * 1024**3,
        disk_required=6 * 1024**3,
        cores_required=1,
    ):
        self.project_service = ACliService(
            job_token,
            cli_base_url=env.ACCELERATOR_CLI_BASE_URL
        )

        self.bucket_object_id = bucket_object_id

        self.ram_required = ram_required
        self.disk_required = disk_required
        self.cores_required = cores_required

        
        self()
        

    def get_file_size(self) -> int:
        result = self.project_service.get_file_stat(self.bucket_object_id)
        return result.get('_size')

    def verify_with_less_disk_less_ram(self):
        raise NotImplementedError(
            "Not implemented with less disk and less ram than required by filesize."
        )

    def verify_with_less_disk_more_ram(self):
        raise NotImplementedError(
            "Not implemented with less disk and more ram than required by filesize."
        )

    def verify_with_more_disk_less_ram(self):
        """First get two chunks that fits into memory. Make both wide.
        Check each index from first chunk in second check.
        If found, delete that index from first chuk and merge its timeseries data in second chunk.

        Upload or save first chunk. Save if you have enough disk space. Upload if not.

        Now get another chunk and do the same with 2nd and newer chunk
        """
        raise NotImplementedError(
            "Not implemented with more disk and less ram than required by filesize."
        )

    def verify_with_more_disk_more_ram(self):
        MoreDiskMoreRamHandler(self)

    def __call__(self):
        file_size = self.get_file_size()

        if (self.disk_required > file_size) and (self.ram_required > file_size):
            self.verify_with_more_disk_more_ram()

        elif (self.disk_required > file_size) and (self.ram_required < file_size):
            self.verify_with_more_disk_less_ram()

        elif (self.disk_required < file_size) and (self.ram_required > file_size):
            self.verify_with_less_disk_more_ram()

        elif (self.disk_required < file_size) and (self.ram_required < file_size):
            self.verify_with_less_disk_less_ram()

        
