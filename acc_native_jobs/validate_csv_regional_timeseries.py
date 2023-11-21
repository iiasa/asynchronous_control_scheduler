class CsvRegionalTimeseriesVerificationService():
    def __init__(
        self,
        *,
        bucket_object_id,
        dataset_template_id,
        job_token,
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
        
        self()

   