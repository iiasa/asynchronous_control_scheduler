import re
import base64
import json
import subprocess
import enum
import sys
from typing import Union
from pathlib import Path
from kubernetes import client, config, dynamic
from kubernetes.client import api_client

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
            base_stack: Union[BaseStack, None]=None,
            force_build=False
        ):
        self.git_repo = git_repo
        self.version = version
        self.dockerfile = dockerfile
        self.base_stack = base_stack
        self.force_build = force_build
        
        if self.tag_exists() and (not self.force_build):
            print(
                f"WKube Builder: Skipping image build as image for given repo and"
                f" tag already exists, force update is turned off."
            )
            return self.get_image_tag()

        try:
            self.prepare_files()
            self.build()
            self.push_to_registry()
            self.clean_up()
        finally:
            self.clear_site()
        
        return self.get_image_tag()
            
    
    def tag_exists(self):
        command = [
            "skopeo", "inspect", "--creds", 
            f"{env.IMAGE_REGISTRY_USER}:{env.IMAGE_REGISTRY_PASSWORD}",
            f"docker://{self.get_image_tag()}"
        ]

        process = subprocess.Popen(command, stdout=sys.stdout)
        process.wait()

        exit_code = process.returncode

        return exit_code == 0

    def prepare_files(self):
        
        command = [
                "git", "clone", 
                "--depth", "1", 
                "--branch", self.version, 
                f"{self.git_repo}",
                self.IMAGE_BUILDING_SITE
            ]

        process = subprocess.Popen(command, stdout=sys.stdout)
        process.wait()
    
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
        
        command = [
                "buildah", "bud", 
                '-t',
                self.get_image_tag(),
                "-f", f"{self.IMAGE_BUILDING_SITE}/{self.dockerfile}",
                self.IMAGE_BUILDING_SITE
            ]

        process = subprocess.Popen(command, stdout=sys.stdout)
        process.wait()

    def push_to_registry(self):

        login_command = [
               "buildah", 
               "login", 
               "--username", env.IMAGE_REGISTRY_USER, 
               "--password", env.IMAGE_REGISTRY_PASSWORD,
               env.IMAGE_REGISTRY_URL
            ]

        login_process = subprocess.Popen(login_command, stdout=sys.stdout)
        login_process.wait()


        push_command = ["buildah", "push", self.get_image_tag()]

        push_process = subprocess.Popen(push_command, stdout=sys.stdout)
        push_process.wait()

    def clean_up(self):
        
        command = [
            "buildah", "rmi", self.get_image_tag()
        ]

        process = subprocess.Popen(command, stdout=sys.stdout)
        process.wait()
    
    def clear_site(self):

        delete_command = [
                "rm", "-rf", self.IMAGE_BUILDING_SITE
            ]

        delete_process = subprocess.Popen(delete_command, stdout=sys.stdout)
        delete_process.wait()

        make_command = [
                "mkdir", self.IMAGE_BUILDING_SITE
            ]

        make_process = subprocess.Popen(make_command, stdout=sys.stdout)
        make_process.wait()


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

        self.image_builder =  OCIImageBuilder()

    def get_service_api(self):
        api_client = dynamic.DynamicClient(
            api_client.ApiClient(configuration=config.load_kube_config_from_dict(
                config_dict=json.loads(
                    base64.b64decode(
                        env.WKUBE_SECRET_JSON_B64.encode()
                    )
                )
            ))
        )

        return api_client
    
    def get_or_create_job_image(self):
        if self.kwargs['docker_image']:
            return self.kwargs['docker_image']

        return self.image_builder(
            self.kwargs['repo_url'],
            self.kwargs['repo_branch'],
            dockerfile=self.kwargs['docker_filename'],
            base_stack=self.kwargs['base_stack'],
            force_build=self.kwargs['force_build']
        )
    
    def get_or_create_cache_pvc(self):
        # Yes it is get or create because parent might already has created.
        """Single node scrach space. 
        Will also be used for data repository cache
        """

        pvc_manifest = dict(
            apiVersion='v1',
            kind='PersistentVolumeClaim',
            metadata=dict(
                name='my-pvc'
            ),
            spec=dict(
                storageClassName='your-storage-class',
                accessModes=['ReadWriteOnce'],
                resources=dict(
                    requests=dict(
                        storage='1Gi'
                    )
                )
            )
        )

        

        # service = api.create(body=service_manifest, namespace=env.WKUBE_K8_NAMESPACE)
        # service_created = api.get(name=name, namespace=env.WKUBE_K8_NAMESPACE)
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
        self.kwargs['docker_image'] = self.get_or_create_job_image()

