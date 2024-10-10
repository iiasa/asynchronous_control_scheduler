import io
import os
import json
import uuid
from typing import Callable, TypedDict, Iterator
from accli import AjobCliService
from dateutil.parser import parse as parse_date
from acc_worker.configs.Environment import get_environment_variables

env = get_environment_variables()


class CSVRegionalTimeseriesMergeService:
    def __init__(
        self,
        *,
        filename: str,
        bucket_object_id_list: list[int],
        job_token
    ):
        
        if not filename:
            raise ValueError("Filename for merged file is required.")


        self.project_service = AjobCliService(
            job_token,
            server_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )

        self.output_filename = filename

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

        filepath = f"{self.temp_downloaded_filepath[:-5]}_{bucket_object_id}.csv" 

        with open(filepath, "wb") as tmp_file:
            for data in response.stream(amt=1024 * 1024):
                size = tmp_file.write(data)

        response.release_conn()
        print('File download complete')

        return filepath
           

    
    def delete_local_file(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    def get_possible_file_line_break(self, filepath):
        breaks = []
        with open(filepath, 'rb') as fl:
            fl.seek(-1, 2)
            breaks.append(fl.read())

            fl.seek(-2, 2)
            breaks.append(fl.read())
        return breaks

        
    def get_merged_validated_metadata(self):
        first_validation_details = self.project_service.get_bucket_object_validation_details(self.bucket_object_id_list[0])

        dataset_template_details = self.project_service.get_dataset_template_details(first_validation_details['dataset_template_id'])

        rules =  dataset_template_details.get('rules')
        
        time_dimension = rules['root_schema_declarations']['time_dimension']

        first_validation_metadata = first_validation_details['validation_metadata']

        for bucket_object_id in self.bucket_object_id_list[1:]:
            next_validation_metadata = self.project_service.get_bucket_object_validation_details(bucket_object_id)['validation_metadata']

            for key in first_validation_metadata:

                if f"{time_dimension.lower()}_meta" not in first_validation_metadata:
                    raise ValueError(f"Revalidate bucket object #{self.bucket_object_id_list[0]}")

                if f"{time_dimension.lower()}_meta" not in next_validation_metadata:
                    raise ValueError(f"Revalidate bucket object #{bucket_object_id}")

                if key.lower() == f"{time_dimension.lower()}_meta":
                    if next_validation_metadata[key]['min_value'] < first_validation_metadata[key]['min_value']:
                        first_validation_metadata[key]['min_value'] = f"{time_dimension.lower()}_meta"[key]['min_value']
                    
                    if next_validation_metadata[key]['max_value'] > first_validation_metadata[key]['max_value']:
                        first_validation_metadata[key]['max_value'] = next_validation_metadata[key]['max_value']
                if key == 'variable-unit':
                    first_merge_candidate = first_validation_metadata[key]
                    next_merge_candidate = next_validation_metadata[key]

                    first_merge_candidate = {tuple(lst) for lst in first_merge_candidate}
                    next_merge_candidate = {tuple(lst) for lst in next_merge_candidate}

                    first_validation_metadata[key] = first_merge_candidate.union(next_merge_candidate)
                else:
                    first_validation_metadata[key] = set(first_validation_metadata[key]).union(set(next_validation_metadata[key]))
        
        return first_validation_metadata, first_validation_details['dataset_template_id']


    def __call__(self):
        self.check_input_files()


        first_downloaded_filepath = self.download_file(self.bucket_object_id_list[0])

        for bucket_object_id in self.bucket_object_id_list[1:]:

            possible_line_breaks = self.get_possible_file_line_break(first_downloaded_filepath)

            next_downloaded_filepath = self.download_file(bucket_object_id)

            with open(first_downloaded_filepath, "ab") as merged_file:
                with open(next_downloaded_filepath, 'rb') as being_merged_file:
                    
                    if not set([b'\n', b'\r\n', b'\r', b'\n\r']).intersection(set(possible_line_breaks)):
                        dat = '\n'
                        merged_file.write(dat)

                    # Skip the first line of the being_merged_file
                    first_line = being_merged_file.readline()

                    while True:
                        dat = being_merged_file.read(1024**2)
                        if not dat:
                            break
                        merged_file.write(dat)

                
                self.delete_local_file(next_downloaded_filepath)
        
        validation_metadata, dataset_template_id = self.get_merged_validated_metadata()

        with open(first_downloaded_filepath, "rb") as file_stream:
            uploaded_bucket_object_id = self.project_service.add_filestream_as_job_output(
                f"{self.output_filename}.csv",
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


          


        