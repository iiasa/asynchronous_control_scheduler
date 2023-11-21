import csv
import uuid
from typing import Optional
from accli import ACliService
from configs.Environment import get_environment_variables
from jsonschema import validate as jsonschema_validate
from jsonschema.exceptions import ValidationError, SchemaError

env = get_environment_variables()

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
        
        self.project_service = ACliService(
            job_token,
            cli_base_url=env.ACCELERATOR_CLI_BASE_URL,
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
        self.temp_dir = f"tmp_files"
        self.temp_downloaded_filepath = (
            f"{self.temp_dir}/{self.temp_downloaded_filename}"
        )
        self.temp_validated_filepath = (
            f"{self.temp_dir}/{self.temp_validated_filename}"
        )
    
    
    def get_map_documents(self, field_name):
        map_documents = self.rules.get(f'Map_{field_name}')
        return map_documents


    def init_validation_metadata(self):
        self.validation_metadata = {
            self.rules['rootSchemaDeclarations']['timeDimension']: {
                "minValue": float('+inf'),
                "maxValue": float('-inf')
            }
        }

        self.harvest_count = {}

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

    def validate_row_data(self, row):
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
        

        for key in self.rules['root']:
            map_documents = self.get_map_documents(key)

            if map_documents:
                if row[key] not in map_documents:
                    raise ValueError(f"{row[key]} must be one of {map_documents.keys()}" )
                
            else:
                if not self.validation_metadata.get(key):
                    if self.harvest_count[key] <= 200: #limit harvest
                        self.validation_metadata[key].add(row[key])
                        self.harvest_count[key] += 1

                else:
                    self.validation_metadata[key] = set([row[key]])
                    self.harvest_count[key] = 0

            if key == self.rules['rootSchemaDeclarations']['timeDimension']:
                if float(row[key]) < self.validation_metadata[
                    self.rules['rootSchemaDeclarations']['timeDimension']
                ]["minValue"]:
                    self.validation_metadata[
                        self.rules['rootSchemaDeclarations']['timeDimension']
                    ]["minValue"] = float(row[key])

                if float(row[key]) < self.validation_metadata[
                    self.rules['rootSchemaDeclarations']['timeDimension']
                ]["maxValue"]:
                    self.validation_metadata[
                        self.rules['rootSchemaDeclarations']['timeDimension']
                    ]["minValue"] = float(row[key])

        extra_template_validators = self.rules.get('templateValidators')

        if extra_template_validators:
                
            for row_key in extra_template_validators.keys():
                lhs = row[row_key]

                condition_object = extra_template_validators[row_key]

                for condition in condition_object.keys():
                
                    rhs_value_pointer = condition_object[condition]

                    rhs = None
                    for pointer in rhs_value_pointer:
                        if pointer.startswith('&'):
                            rhs = self.rule[pointer[1:]]
                        elif pointer.startswith('{') and pointer.endswith('}'):
                            rhs = rhs[pointer[1:-1]]

                        else:
                            rhs = rhs[pointer[1:-1]]


                    if condition == 'valueEquals':
                        if lhs != rhs:
                            raise ValueError(
                                f'{lhs} in {row_key} column must be equal to {rhs}.'
                            )
                    
                    if condition == 'isSubsetOfMap':
                        if not lhs in rhs:
                            raise ValueError(
                                f'{lhs} in {row_key} column must be member of {rhs}.'
                            )

        return row          
    def create_validated_file(self):
        with open(self.temp_downloaded_filepath) as csvfile:
            reader = csv.DictReader(
                csvfile, 
                fieldnames=self.csv_fieldnames, 
                restkey='restkeys', 
                restval='restvals'
            )

            for row in reader:
                row.pop('restkeys', None)
                row.pop('restvals', None)
                self.validate_row_data(row)

    def replace_file_content(self, local_file_path, bucket_object_id):
        with open(local_file_path, "rb") as file_stream:
            bucket_object_id = self.project_service.replace_bucket_object_id_content(
                bucket_object_id,
                file_stream,
            )
        return bucket_object_id
                
    def __call__(self):
        self.download_file()
        self.set_csv_regional_validation_rules()

        self.init_validation_metadata()
        self.create_validated_file()

        print('File validated against rules.')
        self.replace_file_content(self.temp_validated_filepath, self.bucket_object_id)
        print('File replaced')
        self.project_service.register_validation(
            self.bucket_object_id,
            self.dataset_template_id,
            self.validation_metadata
        )
        print('Validation complete')

   