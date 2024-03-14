import re
import subprocess
import enum
from typing import Union
from pathlib import Path

from accli import AjobCliService

from configs.Environment import get_environment_variables
env = get_environment_variables()

class BaseStack(str, enum.Enum):
    PYTHON3_7 = 'PYTHON3_7'
    GAMS_W_R = 'GAMS_W_R'

class OCIImageBuilder:
    """Build image based on Dockerfile in git repo or
       base stack choosen
    """

    IMAGE_BUILDING_SITE = "image_building_site"

    def __call__(
            self, 
            git_repo, 
            version, 
            dockerfile='Dockerfile', 
            base_stack: Union[BaseStack, None]=None
        ):
        self.git_repo = git_repo
        self.version = version
        self.dockerfile = dockerfile
        self.base_stack = base_stack
        
        try:
            self.prepare_files()
            self.build()
            self.push_to_registry()
            self.clean_up()
        finally:
            self.clear_site()
            pass
            
    

    def prepare_files(self):
        # Step 1. Clone git_repo with version.
        subprocess.run(
            [
                "git", "clone", 
                "--depth", "1", 
                "--branch", self.version, 
                f"{self.git_repo}",
                self.IMAGE_BUILDING_SITE
            ],
            check=True
        )
    
        # Step 2. Either dockerfile should be present or base_stack should be choosen
        if not (self.dockerfile or self.base_stack):
            raise ValueError("Either dockerfile of base_stack should be present with the job")
        
        if self.dockerfile:
            dockerfile_path = Path(f"{self.IMAGE_BUILDING_SITE}/{self.dockerfile}")
            if not dockerfile_path.is_file():
                raise ValueError(f"{dockerfile_path} does not exists")
        else:
            self.create_dockerfile_for_basestack()

    def create_dockerfile_for_basestack(self):
        """Create a dockerfile and set the value of self.dockerfile
        """
        raise NotImplementedError('Dockerfile creation of base_stack not implemented')
        

    def get_image_tag(self):
        
        url = self.git_repo
        if url.startswith('http'):
            url = re.sub(r'https?://', '', url)
        if url.startswith('www.'):
            url = re.sub(r'www.', '', url)

        if url.endswith(".git"):
            url = url[:-4]
        return f"registry.iiasa.ac.at/accelerator/{url}:{self.version}"

    
    def build(self):
        subprocess.run(
            [
                "buildah", "bud", 
                '-t',
                self.get_image_tag(),
                "-f", f"{self.IMAGE_BUILDING_SITE}/{self.dockerfile}",
                self.IMAGE_BUILDING_SITE
            ],
            check=True
        )

    def push_to_registry(self):
        subprocess.run(
            [
               "buildah", 
               "login", 
               "--username", env.IMAGE_REGISTRY_USER, 
               "--password", env.IMAGE_REGISTRY_PASSWORD,
               env.IMAGE_REGISTRY_URL
            ],
            check=True
        )


        subprocess.run(
            ["buildah", "push", self.get_image_tag()],
            check=True
        )

    def clean_up(self):
        subprocess.run([
            "buildah", "rmi", self.get_image_tag()
        ], check=True)
    
    def clear_site(self):

        subprocess.run(
            [
                "rm", "-rf", self.IMAGE_BUILDING_SITE
            ],
            check=True
        )

        subprocess.run(
            [
                "mkdir", self.IMAGE_BUILDING_SITE
            ],
            check=True
        )


BuildOCIImage = OCIImageBuilder()

# class WKubeTask():
#     def __init__(self, serialized_task: dict):
#         pass
    
class DispachWkubeTask():

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        job_token = kwargs['job_token']    
        self.project_service = AjobCliService(
            job_token,
            job_cli_base_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )

    def check_if_the_task_is_ready(self, parent_job_id=None, predecessor_job_id=None):
        if (not parent_job_id) and (not predecessor_job_id):
            return True
        
        if parent_job_id:
            pass

        if predecessor_job_id:
            # check both predessor and its children
            pass
    
    def get_or_create_cache_pv(self):
        """Single node scrach space. 
        Will also be used for data repository cache
        """
        pass

    def attach_pvc_to_job(self):
        """
        Attach only when it is child or has child. 
        If it is standalone task than no pvc is required.
        """
        pass

    def override_cmd_ingest_log(self):
        pass

    def __call__(self):
        pass