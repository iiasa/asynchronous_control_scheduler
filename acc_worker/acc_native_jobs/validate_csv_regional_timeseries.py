import json
import os
import subprocess
import csv
import uuid
import itertools
from typing import Optional
from accli import AjobCliService
from acc_worker.configs.Environment import get_environment_variables
from jsonschema import validate as jsonschema_validate
from jsonschema.exceptions import ValidationError, SchemaError

env = get_environment_variables()


class CaseInsensitiveDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key.lower(), value)

    def __getitem__(self, key):
        return super().__getitem__(key.lower())

    def __delitem__(self, key):
        super().__delitem__(key.lower())

    def __contains__(self, key):
        return super().__contains__(key.lower())

    def get(self, key, default=None):
        return super().get(key.lower(), default)

    def pop(self, key, default=None):
        return super().pop(key.lower(), default)

    def update(self, *args, **kwargs):
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def setdefault(self, key, default=None):
        return super().setdefault(key.lower(), default)

# Example usage:
case_insensitive_dict = CaseInsensitiveDict({'A': 1, 'b': 2})
print(case_insensitive_dict['a'])  # Output: 1
print(case_insensitive_dict['B'])  # Output: 2

case_insensitive_dict['C'] = 3
print(case_insensitive_dict['c'])  # Output: 3


def lower_rows(iterator):
    # return itertools.chain([next(iterator).lower()], iterator)
    for item in iterator:
        yield item.lower()

class CsvRegionalTimeseriesVerificationService():
    def __init__(
        self,
        *,
        bucket_object_id,
        dataset_template_id,
        job_token,
        csv_fieldnames: Optional[list[str]]=None,
        ram_required=4 * 1024**3,
        disk_required=6 * 1024**3,
        cores_required=1,
    ):
        
        self.project_service = AjobCliService(
            job_token,
            server_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )   

        self.dataset_template_id = dataset_template_id

        self.bucket_object_id = bucket_object_id

        self.ram_required = ram_required
        self.disk_required = disk_required
        self.cores_required = cores_required

        self.csv_fieldnames = csv_fieldnames

        self.temp_downloaded_filename = f"{uuid.uuid4().hex}.csv"
        self.temp_validated_filename = f"{uuid.uuid4().hex}.csv"
        self.temp_sorted_filename = f"{uuid.uuid4().hex}.csv"
        self.temp_dir = f"tmp_files"
        self.temp_downloaded_filepath = (
            f"{self.temp_dir}/{self.temp_downloaded_filename}"
        )
        self.temp_validated_filepath = (
            f"{self.temp_dir}/{self.temp_validated_filename}"
        )

        self.temp_sorted_filepath = (
            f"{self.temp_dir}/{self.temp_sorted_filename}"
        )

        self.errors = dict()
    
    
    def get_map_documents(self, field_name):
        map_documents = self.rules.get(f'map_{field_name}')
        return map_documents


    def init_validation_metadata(self):
        self.validation_metadata = {
            f"{self.time_dimension}_meta": {
                "min_value": float('+inf'),
                "max_value": float('-inf')
            }
        }

    def download_file(self):
        print('Downloading file to validate.')
        response = self.project_service.get_file_stream(
            self.bucket_object_id
        )

        with open(self.temp_downloaded_filepath, "wb") as tmp_file:
            for data in response.stream(amt=1024 * 1024):
                size = tmp_file.write(data)

        response.release_conn()
        print('File download complete')

    def set_csv_regional_validation_rules(self):
        dataset_template_details = self.project_service.get_dataset_template_details(self.dataset_template_id)
        self.rules =  dataset_template_details.get('rules')


        assert self.rules, \
            f"No dataset template rules found for dataset_template id: \
                {self.dataset_template_id}"
        
        self.time_dimension = self.rules['root_schema_declarations']['time_dimension']
        self.value_dimension = self.rules['root_schema_declarations']['value_dimension']

        self.unit_dimension = self.rules['root_schema_declarations']['unit_dimension']
        self.variable_dimension = self.rules['root_schema_declarations']['variable_dimension']

        self.region_dimension = self.rules['root_schema_declarations']['region_dimension']


    def validate_row_data(self, row):
        row = CaseInsensitiveDict(row)
        try:
            jsonschema_validate(
                self.rules.get('root'),
                row
            )

        except SchemaError as schema_error:
           
            raise ValueError(
                f"Schema itself is not valid with template id. Template id: {self.dataset_template_id}. Original exception: {str(schema_error)}"
            )
        except ValidationError as validation_error:
            raise ValueError(
                f"Invalid data. Template id: {self.dataset_template_id}. Data: {str(validation_error)}. Original exception: {str(validation_error)}"
            )
        

        for key in self.rules['root']['properties']:

            if key in [self.variable_dimension, self.unit_dimension]:
                continue

            if key == self.time_dimension:
                if float(row[key]) < self.validation_metadata[
                    f"{self.time_dimension}_meta"
                ]["min_value"]:
                    self.validation_metadata[
                        f"{self.time_dimension}_meta"
                    ]["min_value"] = float(row[key])

                if float(row[key]) > self.validation_metadata[
                    f"{self.time_dimension}_meta"
                ]["max_value"]:
                    self.validation_metadata[
                        f"{self.time_dimension}_meta"
                    ]["max_value"] = float(row[key])

            if key == self.value_dimension:
                continue


            map_documents = self.get_map_documents(key)

            if map_documents:
                if row[key] not in map_documents:
                    raise ValueError(f"'{row[key]}' must be one of {map_documents.keys()}" )
                
        
            if self.validation_metadata.get(key):
                if len(self.validation_metadata[key]) <= 1000: #limit harvest
                    self.validation_metadata[key].add(row[key])

            else:
                self.validation_metadata[key] = set([row[key]])

        if self.validation_metadata.get('variable-unit'):
            if len(self.validation_metadata['variable-unit']) <= 1000: #limit harvest
                    self.validation_metadata['variable-unit'].add((row[self.variable_dimension], row[self.unit_dimension]))
        else:
            self.validation_metadata['variable-unit'] = set([
                (row[self.variable_dimension], row[self.unit_dimension])
            ])


        extra_template_validators = self.rules.get('template_validators')

        if extra_template_validators and extra_template_validators != 'not defined':
                
            for row_key in extra_template_validators.keys():
                lhs = row[row_key]

                condition_object = extra_template_validators[row_key]

                for condition in condition_object.keys():
                
                    rhs_value_pointer = condition_object[condition]

                    rhs = None
                    for pointer in rhs_value_pointer:
                        if pointer.startswith('&'):
                            rhs = self.rules[pointer[1:]]
                        elif pointer.startswith('{') and pointer.endswith('}'):
                            rhs = rhs[row[pointer[1:-1]]]

                        else:
                            rhs = rhs[pointer]


                    if condition == 'value_equals':
                        if lhs != rhs:
                            raise ValueError(
                                f'{lhs} in {row_key} column must be equal to {rhs}.'
                            )
                    
                    if condition == 'is_subset_of_map':
                        if not lhs in rhs:
                            raise ValueError(
                                f'{lhs} in {row_key} column must be member of {rhs}.'
                            )

        return row          
    def get_validated_rows(self):
        with open(self.temp_downloaded_filepath) as csvfile:
            reader = csv.DictReader(
                lower_rows(csvfile), 
                fieldnames=self.csv_fieldnames, 
                restkey='restkeys', 
                restval='restvals'
            )

            for row in reader:
                row.pop('restkeys', None)
                row.pop('restvals', None)

                try:
                    row = self.validate_row_data(row)
                except Exception as err:
                    if len(self.errors) <= 50:
                        self.errors[str(err)] = str(row)

                yield row
    
    def create_validated_file(self):
        with open(self.temp_validated_filepath, 'w') as csv_validated_file:

            # Prepare final header order
            headers = self.rules['root']['properties'].copy()

            self.validated_headers = []

            final_dimensions_order = self.rules['root_schema_declarations'].get('final_dimensions_order')

            if final_dimensions_order:
                for item in final_dimensions_order:
                    if item in headers:
                        if item not in [self.time_dimension, self.value_dimension]:
                            self.validated_headers.append(item)
            else:
                raise ValueError("'final_dimensions_order' in template is required")
            
            for item in headers:
                used_headers = self.validated_headers + [self.time_dimension, self.value_dimension]
                if item not in used_headers:
                    self.validated_headers.append(item)
            
            self.validated_headers = self.validated_headers + [self.time_dimension, self.value_dimension]
            # End final order preparation

            
            writer = csv.DictWriter(csv_validated_file, fieldnames=self.validated_headers, extrasaction='ignore')

            writer.writeheader()
            
            validated_rows = self.get_validated_rows()

            for row in validated_rows:
            
                writer.writerow(row)
        

    def replace_file_content(self, local_file_path, bucket_object_id):
        with open(local_file_path, "rb") as file_stream:
            bucket_object_id = self.project_service.replace_bucket_object_id_content(
                bucket_object_id,
                file_stream,
            )
        return bucket_object_id
    
    def delete_local_file(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)
                
    def __call__(self):
        self.download_file()
        self.set_csv_regional_validation_rules()

        self.init_validation_metadata()
        
        try:
            self.create_validated_file()
            print('File validated against rules.')
        finally:
            self.delete_local_file(self.temp_downloaded_filepath)
            print('Temporary downloaded file deleted')

        if self.errors:
            for key in self.errors:
                print(f"Invalid data: {self.errors[key]}")
                print(f"Error: {key}")
            self.delete_local_file(self.temp_validated_filepath)
            print('Temporary validated file deleted')
            raise ValueError("Invalid data: Data not comply with template rules.")


        sort_order_option_text = ' '.join([f"-k{i+1},{i+1}{'n' if self.validated_headers[i] == self.time_dimension else ''}" for i in range(len(self.validated_headers[:-1]))])

        sort_command = f"head -n1 {self.temp_validated_filepath} >> {self.temp_sorted_filepath} && tail -n+2 {self.temp_validated_filepath} | sort -t',' {sort_order_option_text} >> {self.temp_sorted_filepath}"

        print(sort_command)
        print(self.validated_headers)

        subprocess.run(
            sort_command,
            capture_output=True,
            shell=True
        )
        print("Validated file sorted")

        self.delete_local_file(self.temp_validated_filepath)
        print('Temporary validated file deleted')

        self.replace_file_content(self.temp_sorted_filepath, self.bucket_object_id)
        print('File replaced')

        # Monkey patch serializer
        def monkey_patched_json_encoder_default(encoder, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(encoder, obj)

        json.JSONEncoder.default = monkey_patched_json_encoder_default
        # Monkey patch serializer


        self.project_service.register_validation(
            self.bucket_object_id,
            self.dataset_template_id,
            self.validation_metadata
        )
        print('Validation complete')

        print(self.temp_sorted_filepath)
        self.delete_local_file(self.temp_sorted_filepath)
        print('Temporary sorted file deleted')

   