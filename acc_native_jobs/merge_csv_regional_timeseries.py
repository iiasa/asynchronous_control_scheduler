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


class CSVRegionalTimeseriesMergeService:
    def __init__(
        self,
        *,
        bucket_object_id_list: list[int],
        job_token
    ):

        self.project_service = ACliService(
            job_token,
            cli_base_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )

        self.bucket_object_id_list = bucket_object_id_list

        self.temp_downloaded_filename = f"{uuid.uuid4().hex}.csv"
        # self.temp_merged_filename = f"{uuid.uuid4().hex}.csv"
        self.temp_dir = f"tmp_files"
        
        self.temp_downloaded_filepath = (
            f"{self.temp_dir}/{self.temp_downloaded_filename}"
        )
        
        # self.temp_merged_filepath = (
        #     f"{self.temp_dir}/{self.temp_merged_filename}"
        # )

    
    def check_input_files(self):
        if not isinstance(self.bucket_object_id_list, list):
            raise ValueError("Argument 'bucker_object_id_list' should be list.")
        
        if len(self.bucket_object_id_list) < 2:
            raise ValueError("Argument 'bucker_object_id_list' at least two items.")
        
        first_file_type_id = self.project_service.get_bucket_object_validation_type(
            self.bucket_object_id_list[0]
        )
        
        for bucket_object_id in self.bucket_object_id_list[1:]:
            other_file_type_id = self.project_service.get_bucket_object_validation_type(
                bucket_object_id
            )

            if first_file_type_id != other_file_type_id:
                raise ValueError(
                    "Arguments 'bucker_object_id_list' should be of same dataset template"
                )
            
    def download_file(self, bucket_object_id):
        print('Downloading file to validate.')
        response = self.project_service.get_file_stream(
            bucket_object_id
        )

        filepath = f"{self.temp_downloaded_filepath}_{bucket_object_id}" 

        with open(filepath, "wb") as tmp_file:
            for data in response.stream(amt=1024 * 1024):
                size = tmp_file.write(data)

        response.release_conn()
        print('File download complete')

        return filepath
           

    
    def delete_local_file(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    def get_last_byte_of_file(self, filepath):
        with open(filepath, 'rb') as fl:
            fl.seek(-1, 2)
            return fl.read()
        
    def get_merged_validated_metadata(self):
        first_validation_details = self.project_service.get_bucket_object_validation_details(self.bucket_object_id_list[0])

        dataset_template_details = self.project_service.get_dataset_template_details(first_validation_details['dataset_template_id'])

        rules =  dataset_template_details.get('rules')
        
        time_dimension = rules['root_schema_declarations']['time_dimension']

        first_validation_metadata = first_validation_details['validation_metadata']

        for bucket_object_id in self.bucket_object_id_list[1:]:
            next_validation_metadata = self.project_service.get_bucket_object_validation_details(bucket_object_id)['validation_metadata']

            for key in first_validation_metadata:
                if key.lower() == time_dimension.lower():
                    if next_validation_metadata[key]['min_value'] < first_validation_metadata[key]['min_value']:
                        first_validation_metadata[key]['min_value'] = next_validation_metadata[key]['min_value']
                    
                    if next_validation_metadata[key]['max_value'] > first_validation_metadata[key]['max_value']:
                        first_validation_metadata[key]['max_value'] = next_validation_metadata[key]['max_value']
                else:
                    first_validation_metadata[key] = set(first_validation_metadata[key]).union(set(next_validation_metadata[key]))
        
        return first_validation_metadata, first_validation_details['dataset_template_id']


    def __call__(self):
        self.check_input_files()


        first_downloaded_filepath = self.download_file(self.bucket_object_id_list[0])

        for bucket_object_id in self.bucket_object_id_list[1:]:

            current_end_byte = self.get_last_byte_of_file(first_downloaded_filepath)

            next_downloaded_filepath = self.download_file(bucket_object_id)

            with open(first_downloaded_filepath, "wb") as merged_file:
                with open(next_downloaded_filepath, 'rb') as being_merged_file:
                    dat = being_merged_file.read(1024**2)
                    if current_end_byte != b'\n':
                        dat = '\n' + dat
                    
                    merged_file.write(dat)
                
                self.delete_local_file(next_downloaded_filepath)
        
        validation_metadata, dataset_template_id = self.get_merged_validated_metadata()

        with open(first_downloaded_filepath, "rb") as file_stream:
            uploaded_bucket_object_id = self.project_service.add_filestream_as_job_output(
                f"Merged_{self.temp_downloaded_filename[:7]}",
                file_stream,
            )

        # Monkey patch serializer
        def monkey_patched_json_encoder_default(encoder, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(encoder, obj)

        json.JSONEncoder.default = monkey_patched_json_encoder_default
        # Monkey patch serializer


        self.project_service.register_validation(
            uploaded_bucket_object_id,
            dataset_template_id,
            validation_metadata
        )
        print('Merge complete')

        self.delete_local_file(first_downloaded_filepath)
        print('Temporary sorted file deleted')


          


        