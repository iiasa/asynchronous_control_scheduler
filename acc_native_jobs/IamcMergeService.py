import io
import os
import json
import uuid
import pyam
from typing import Callable, TypedDict, Iterator
from accli import AjobCliService
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



class IamcMergeService:
    def __init__(
        self,
        *,
        bucket_object_id_list: list[int],
        job_token
    ):

        self.temp_dir = f"background_tasks/temp"

        self.project_service = AjobCliService(
            job_token,
            cli_base_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )


        self.project_service.update_job_status("PROCESSING")

        # with remote job user will be denoted by token

        self.bucket_object_id_list = bucket_object_id_list

        self.validated_meta_db = None

        try:
            self()
        except Exception as err:
            self.project_service.update_job_status("ERROR")
            raise err

    def remove_return_from_bytes(self, bytes_str):
        if bytes_str.endswith(b"\n"):
            bytes_str = bytes_str[0:-1]

        if bytes_str.endswith(b"\r"):
            bytes_str = bytes_str[0:-1]

        return bytes_str

    def get_headers_of_all_to_be_merged_objects(self) -> list[bytes]:
        all_objects_headers: list[bytes] = []
        for bucket_object_id in self.bucket_object_id_list:
            try:
                response = self.project_service.get_file_stream(
                    bucket_object_id, self.job_id
                )
                all_objects_headers.append(response.readline())
            finally:
                response.close()
                response.release_conn()

        return all_objects_headers

    def check_individual_file_iamc_validation(self):
        for bucket_object_id in self.bucket_object_id_list:
            type_name = self.project_service.get_bucket_object_validation_type(
                bucket_object_id
            )
            if type_name != "IAMC":
                return False
        return True

    def validate_header_order_get_merged_timeseries_template(
        self, datasets_headers_bytes: list[bytes]
    ):
        datasets_headers_columns = []
        for bytes_data in datasets_headers_bytes:
            datasets_headers_columns.append(
                self.remove_return_from_bytes(bytes_data).split(b",")
            )

        datasets_iamc_header_columns = []
        datasets_timeseries_columns = []

        for dataset_header_columns in datasets_headers_columns:
            iamc_header = []
            timeseries = []

            for column in dataset_header_columns:
                try:
                    date = parse_date(column)
                    timeseries.append(column)
                except:
                    iamc_header.append(column.strip().lower())

            datasets_iamc_header_columns.append(iamc_header)
            datasets_timeseries_columns.append(timeseries)

        if not datasets_iamc_header_columns.count(
            datasets_iamc_header_columns[0]
        ) == len(datasets_iamc_header_columns):
            error_message = (
                f"Headers are not consistent across files"
                f"Headers are: {datasets_iamc_header_columns}"
            )
            raise ValueError(error_message)

        self.merged_ordered_timeseries_header_template = dict()
        self.validated_dimension_ordered_headers = datasets_iamc_header_columns[0]

        for dataset_timeseries_columns in datasets_timeseries_columns:
            for column in dataset_timeseries_columns:
                self.merged_ordered_timeseries_header_template[column] = b""

    def append_return_to_byte(self, data: bytes):
        if not data.endswith(b"\n"):
            return data + b"\n"
        return data

    def get_csv_rows_from_remote_connection(self) -> Iterator[tuple[bytes, int]]:
        for idx, bucket_object_id in enumerate(self.bucket_object_id_list):
            try:
                response = self.project_service.get_file_stream(
                    bucket_object_id, self.job_id
                )
                while True:
                    line = response.readline()
                    if not line:
                        break
                    yield self.append_return_to_byte(line), idx
            finally:
                response.close()
                response.release_conn()

    def merge_iamc(self, csv_stream: Iterator[tuple[bytes, int]]):
        current_file_index = None

        # used for creating index db (year is missing)
        self.merged_iamc_field_options: list[set] = [set() for i in range(len(
            self.validated_dimension_ordered_headers
        ))]

        # add year
        self.merged_iamc_field_options.append(
            set([item.decode() for item in self.merged_ordered_timeseries_header_template.keys()])
        )

        for csv_row_bytes, file_index in csv_stream:
            if file_index != current_file_index:
                # csv_row_bytes is header  of each file
                current_file_timeseries_headers = self.remove_return_from_bytes(
                    csv_row_bytes
                ).split(b",")[len(self.validated_dimension_ordered_headers) :]

                if file_index == 0:
                    # csv_row_bytes is header bytes of first file
                    dimension_headers = b",".join(
                        self.validated_dimension_ordered_headers
                    )
                    timeseries_headers = b",".join(
                        self.merged_ordered_timeseries_header_template.keys()
                    )
                    final_header = dimension_headers + b"," + timeseries_headers + b"\n"
                    yield final_header.title()
                else:
                    # csv_row_bytes is header bytes of other than first files
                    current_file_index = file_index
                    continue

                current_file_index = file_index
            else:
                # row with value
                current_dimension_values = self.remove_return_from_bytes(
                    csv_row_bytes
                ).split(b",")[: len(self.validated_dimension_ordered_headers)]
                current_timeseries_values = self.remove_return_from_bytes(
                    csv_row_bytes
                ).split(b",")[len(self.validated_dimension_ordered_headers) :]
                current_timeseries_template = dict(
                    zip(current_file_timeseries_headers, current_timeseries_values)
                )

                merged_timeseries_dict = (
                    self.merged_ordered_timeseries_header_template.copy()
                )
                merged_timeseries_dict.update(current_timeseries_template)
                final_row = (
                    b",".join(current_dimension_values)
                    + b","
                    + b",".join(merged_timeseries_dict.values())
                    + b"\n"
                )


                for val_index in range(len(current_dimension_values)):

                    self.merged_iamc_field_options[val_index].add(
                        current_dimension_values[val_index].decode()
                    )

                yield final_row


    def create_merged_iamc_index_db(self):
        
        self.meta_data = dict(
            filename=f"{uuid.uuid4().hex}.json",
            data=[]
        )

        all_headers = self.validated_dimension_ordered_headers + [b'year']

        self.meta_data['data'] += [dict(table='csv_headers', name=header.decode()) for header in all_headers]

        
        for header_index in range(len(all_headers)):

            if all_headers[header_index] not in [
                b"value",
                # "unit"
            ]:
                self.meta_data['data'] += [dict(table=all_headers[header_index].decode(), name=value) for value in self.merged_iamc_field_options[header_index]]

        with open(f"{self.temp_dir}/{self.meta_data['filename']}", 'w') as fp:
            for row in self.meta_data['data']:
                json.dump(row, fp)
                fp.write('\n')
           
           

    def upload_index_db_file(self, local_file_path, bucket_filename):
        with open(local_file_path, "rb") as file_stream:
            bucket_object_id = self.project_service.add_filestream_as_job_output(
                bucket_filename,
                file_stream,
                self.user_id,
                self.job_id,
            )

        return bucket_object_id
    
    def upload_csv_stream(self, csv_bytes_stream: Iterator[bytes]):
        filename = f"merged_iamc.csv"

        file_stream = iterable_to_stream(csv_bytes_stream)

        bucket_object_id = self.project_service.add_filestream_as_job_output(
                filename,
                file_stream,
                self.user_id,
                self.job_id,
            )
        return bucket_object_id
    
    def delete_local_file(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)


    def __call__(self):
        self.check_individual_file_iamc_validation()

        datasets_headers_bytes = self.get_headers_of_all_to_be_merged_objects()

        self.validate_header_order_get_merged_timeseries_template(
            datasets_headers_bytes
        )

        csv_stream = self.get_csv_rows_from_remote_connection()

        merged_stream = self.merge_iamc(csv_stream)

        # sink stream
        merged_bucket_object_id = self.upload_csv_stream(merged_stream)
        try:
            self.create_merged_iamc_index_db()

            index_bucket_object_id = self.upload_index_db_file(f"{self.temp_dir}/{self.meta_data['filename']}", self.meta_data['filename'])

            self.project_service.register_iamc_validation(
                merged_bucket_object_id, index_bucket_object_id, self.job_id
            )
        finally:
            self.delete_local_file(f"{self.temp_dir}/{self.meta_data['filename']}")

        self.project_service.update_job_status("DONE")
