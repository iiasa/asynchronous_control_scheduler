import re
import os
import base64
import json
import subprocess
import enum
import sys
import uuid
import time
import shutil
import zipfile
from typing import Union
from pathlib import Path
from celery import current_task
from minio import Minio
from datetime import datetime, timedelta
from kubernetes import client, config, dynamic
from kubernetes.client import api_client
from celery import current_task

from kubernetes.dynamic.exceptions import NotFoundError

from accli import AjobCliService

from acc_worker.configs.Environment import get_environment_variables
from acc_worker.acc_native_jobs.exceptions import WkubeRetryException
from .registries import DEFAULT_REGISTRIES, create_user_registry_secret

env = get_environment_variables()

FOLDER_JOB_REPO_URL = 'https://github.com/IIASA-Accelerator/wkube-job.git'


def escape_character(input_string, char_to_escape):
    """
    Escapes occurrences of a given character within a string.

    Parameters:
    input_string (str): The input string.
    char_to_escape (str): The character to escape.

    Returns:
    str: The string with the specified character escaped.
    """
    # Escape the given character
    escaped_char = '\\' + char_to_escape
    
    # Use built-in string replace function to escape the character
    escaped_string = input_string.replace(char_to_escape, escaped_char)

    return escaped_string

class BaseStack(str, enum.Enum):
    """For each stack below a dockerfile should be present in 
    predefined stacks in acc_worker/k8_gateway_actions/predefined_stacks 
    folder with the following nameing convension 'Dockerfile.<stackname>'

    Each stack may demand some kind of stack native file to be present in
    job or project folder.
    """
    PYTHON3_7 = 'PYTHON3_7'

def exec_command(command, raise_exception=True):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Read and print the output
    for line in process.stdout:
        print(line.decode().strip())

    # Read and print the error
    for line in process.stderr:
        print(line.decode().strip())

    # Optionally, you can wait for the process to finish
    process.wait()

    if raise_exception and process.returncode != 0:
        raise ValueError(f"Something went wrong with the command: '{command}'. Failed to build image.")

    return process.returncode

class OCIImageBuilder:
    """Build image based on Dockerfile in git repo or
       base stack choosen
    """

    IMAGE_BUILDING_SITE = "image_building_site"
    PREDEFINED_STACKS_FOLDER = "acc_worker/k8_gateway_actions/predefined_stacks"

    def __call__(
            self, 
            git_repo, 
            version, 
            job_secrets={},
            dockerfile=None, 
            base_stack: Union[BaseStack, None]=None,
            force_build=False
        ):
        self.git_repo = git_repo.lower()
        self.version = version
        self.dockerfile = dockerfile
        self.base_stack = base_stack
        self.force_build = force_build
        self.job_secrets = job_secrets

        self.set_image_building_site()

        self.dockerfile_path = f"{self.IMAGE_BUILDING_SITE}/Dockerfile"
        
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
    
    def set_image_building_site(self):
        unq_folder = str(uuid.uuid4())
        self.IMAGE_BUILDING_SITE = f"{self.IMAGE_BUILDING_SITE}/{unq_folder}"
            
    
    def tag_exists(self):
        command = [
            "skopeo", "inspect", "--tls-verify=false", "--creds", 
            f"{env.IMAGE_REGISTRY_USER}:{env.IMAGE_REGISTRY_PASSWORD}",
            f"docker://{self.get_image_tag()}"
        ]

        exit_code = exec_command(command, raise_exception=False)

        return exit_code == 0
    
    def get_git_pull_url(self):
        username = self.job_secrets.get('ACC_WKUBE_GIT_USER', None)
        password = self.job_secrets.get('ACC_WKUBE_GIT_PASSWORD', None)

        if username and password:
            return f"https://{username}:{password}@{self.git_repo.split('https://')[1]}"
        else:
            if self.git_repo == env.FOLDER_JOB_REPO_URL:
                raise ValueError(f"ACC_WKUBE_GIT_USER and ACC_WKUBE_GIT_PASSWORD job_secrets are required and supposed to be set by accelerator gateway.")

    def pull_files_from_git(self):
        command = [
                "git", "clone", 
                "--depth", "1", 
                "--branch", self.version, 
                f"{self.get_git_pull_url()}",
                self.IMAGE_BUILDING_SITE
            ]

        exec_command(command)

    def pull_files_from_job_store(self):

        s3_endpoint = env.JOBSTORE_S3_ENDPOINT

        if s3_endpoint.startswith('https://'):
            s3_endpoint = s3_endpoint.split("https://")[1]
        elif s3_endpoint.startswith("http://"):
            s3_endpoint = s3_endpoint.split("https://")[1]

        client = Minio(
            s3_endpoint,
            access_key=env.JOBSTORE_S3_API_KEY,
            secret_key=env.JOBSTORE_S3_SECRET_KEY,
            secure=env.JOBSTORE_S3_ENDPOINT.startswith('https'),
            region=env.JOBSTORE_S3_REGION,
            cert_check=False
            # http_client=http_client,
        )

        remote_filename = self.git_repo.split("s3accjobstore://")[-1]

        downloaded_filepath = f"{self.IMAGE_BUILDING_SITE}/{remote_filename}"

        client.fget_object(
            env.JOBSTORE_S3_BUCKET_NAME,
            remote_filename,
            downloaded_filepath
        )

        if not zipfile.is_zipfile(downloaded_filepath):
            raise ValueError(f"{downloaded_filepath} is not a valid zip file")
    
        parent_dir = os.path.dirname(downloaded_filepath)

        with zipfile.ZipFile(downloaded_filepath, 'r') as zip_ref:
            zip_ref.extractall(parent_dir)
        
        os.remove(downloaded_filepath)
    
    def prepare_files(self):

        if self.git_repo.startswith("s3accjobstore://"):
            self.pull_files_from_job_store()
        
        else:
            self.pull_files_from_git()

    
        # Step 2. Either dockerfile should be present or base_stack should be choosen
        if not (self.dockerfile or self.base_stack):
            raise ValueError("Either dockerfile of base_stack should be present with the job")
        
        if self.dockerfile:
            dockerfile_path = Path(f"{self.IMAGE_BUILDING_SITE}/{self.dockerfile}")
            if not dockerfile_path.is_file():
                raise ValueError(f"{dockerfile_path} does not exists")
            
            self.dockerfile_path = str(dockerfile_path)
        else:
            self.create_dockerfile_for_basestack()

    def create_dockerfile_for_basestack(self):
        """Create a dockerfile and set the value of self.dockerfile
        """

        if not hasattr(BaseStack, self.base_stack):
            raise ValueError(f"'{self.base_stack}' is not defined by WKUBE.")

        base_stack_dockerfile_path = Path(f"{self.PREDEFINED_STACKS_FOLDER}/Dockerfile.{self.base_stack}")
        if not base_stack_dockerfile_path.is_file():
            raise ValueError(
                f"Dockerfile for predefined stack "
                f"'{self.base_stack}' does not exists. Please contact developer."
            )
        self.dockerfile_path = str(base_stack_dockerfile_path)
        
        

    def get_image_tag(self):
        
        url = self.git_repo
        if url.startswith('http'):
            url = re.sub(r'https?://', '', url)
        if url.startswith('www.'):
            url = re.sub(r'www.', '', url)

        if url.startswith('s3accjobstore://'):
            url = re.sub(r's3accjobstore://', '', url)

        if url.endswith(".git"):
            url = url[:-4]
        
        if url.endswith(".zip"):
            url = url[:-4]
        return f"{env.IMAGE_REGISTRY_URL}/{env.IMAGE_REGISTRY_TAG_PREFIX}{url}{self.version}:latest"

    
    def build(self):
        
        command = [
                "sudo", 
                "buildah", 
                "bud",
                "--isolation", "chroot", 
                '-t',
                self.get_image_tag(),
                "-f", self.dockerfile_path,
                self.IMAGE_BUILDING_SITE
            ]

        if self.force_build:
            command.insert(5, '--no-cache')

        exec_command(command)

    def push_to_registry(self):

        login_command = [
               "sudo", "buildah", 
               "login", 
               "--tls-verify=false", 
               f"--username={env.IMAGE_REGISTRY_USER}", 
               f"--password={env.IMAGE_REGISTRY_PASSWORD}",
               env.IMAGE_REGISTRY_URL
            ]
        # buildah login --username myregistry --password myregistrypassword registry:8443

        exec_command(login_command)

        push_command = ["sudo", "buildah", "push", "--tls-verify=false",  self.get_image_tag()]

        exec_command(push_command)

    def clean_up(self):
        
        remove_built_image_command = [
            "sudo", "buildah", "rmi", self.get_image_tag()
        ]

        exec_command(remove_built_image_command)

        cleanup_command = [
            "sudo", "buildah", "rmi", "-p"
        ]

        exec_command(cleanup_command)
    
    def clear_site(self):

        delete_command = [
                "rm", "-rf", self.IMAGE_BUILDING_SITE
            ]

        exec_command(delete_command)

        directory = self.__class__.IMAGE_BUILDING_SITE

        now = time.time()

        # Calculate the time for "yesterday"
        yesterday = now - 24*3600

        # Iterate over each item in the directory
        for folder_name in os.listdir(directory):
            folder_path = os.path.join(directory, folder_name)
            
            # Check if the item is a directory
            if os.path.isdir(folder_path):
                # Get the folder's creation time
                creation_time = os.path.getctime(folder_path)
                
                # If the folder was created before yesterday, delete it
                if creation_time < yesterday:
                    print(f"Deleting folder: {folder_path}")
                    shutil.rmtree(folder_path)


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
            server_url=env.ACCELERATOR_CLI_BASE_URL,
            verify_cert=False
        )

        self.api_cli = self.get_service_api()

        self.image_builder =  OCIImageBuilder()

    def get_core_v1_api(self):
        config.load_kube_config_from_dict(
            config_dict=json.loads(
                base64.b64decode(
                    env.WKUBE_SECRET_JSON_B64.encode()
                )
            )
        )

        v1 = client.CoreV1Api()

        return v1

    def get_service_api(self):
        
        config.load_kube_config_from_dict(
            config_dict=json.loads(
                base64.b64decode(
                    env.WKUBE_SECRET_JSON_B64.encode()
                )
            )
        )

        kube_config = client.Configuration().get_default_copy()

        kube_config.verify_ssl = False

        client.Configuration.set_default(kube_config)
        
        api_cli = dynamic.DynamicClient(
            api_client.ApiClient()
        )

        return api_cli

        # Load kubeconfig from base64 encoded JSON string
        # kubeconfig_data = base64.b64decode(env.WKUBE_SECRET_JSON_B64.encode()).decode('utf-8')
        # config_dict = json.loads(kubeconfig_data)
        # config.load_kube_config_from_dict(config_dict)

        # client.Configuration.ssl_ca_cert = './cert.pem'
        # Create dynamic client
        # api_cli = dynamic.DynamicClient(api_client.ApiClient())

        # return api_cli
    
    def get_or_create_job_image(self):
        if self.kwargs['docker_image']:
            return self.kwargs['docker_image']

        return self.image_builder(
            self.kwargs['repo_url'],
            self.kwargs['repo_branch'],
            job_secrets=self.kwargs['job_secrets'],
            dockerfile=self.kwargs['docker_filename'],
            base_stack=self.kwargs['base_stack'],
            force_build=self.kwargs['force_build']
        )
    
    def pvc_exists(self):

        try:
        
            pvc = self.api_cli.resources.get(api_version='v1', kind='PersistentVolumeClaim').get(
                namespace=env.WKUBE_K8_NAMESPACE, name=self.kwargs['pvc_id'])
            if pvc:
                print(f"PVC {self.kwargs['pvc_id']} exists in namespace {env.WKUBE_K8_NAMESPACE}.")
                return True
            else:
                print(f"PVC {self.kwargs['pvc_id']} does not exist in namespace {env.WKUBE_K8_NAMESPACE}.")
                return False
        except NotFoundError:
            return False
    
    
    def get_or_create_cache_pvc(self):
       
        if self.pvc_exists():
            return

        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": self.kwargs['pvc_id']
            },
            "spec": {
                "accessModes": [
                    "ReadWriteMany"
                ],
                "resources": {
                    "requests": {
                        "storage": self.kwargs.get('required_storage_workflow', 1024 * 1024)
                    }
                }
            }
        }

        # Create the PVC
        v1_pvc = self.api_cli.resources.get(api_version='v1', kind='PersistentVolumeClaim')
        created_pvc = v1_pvc.create(namespace=env.WKUBE_K8_NAMESPACE, body=pvc_manifest)

        print("Created PVC:", created_pvc)

    # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md
    def get_node_name(self):
        label_selector = f"pvc_id={self.kwargs['pvc_id']}"

        pod_resource = self.api_cli.resources.get(api_version='v1', kind='Pod')
        pods = pod_resource.get(namespace=env.WKUBE_K8_NAMESPACE, label_selector=label_selector)


        # If any pod matches, return the node name of the first one
        if pods.items:
            print(f"______________________FOLLOW UP NODE NAME {pods.items[0].spec.node_name}")
            return pods.items[0].spec.node_name

    def __call__(self):
        self.kwargs['docker_image'] = self.get_or_create_job_image()

        self.get_or_create_cache_pvc()
        self.launch_k8_job()


    def get_image_pull_secrets(self):
        
        registires_name = DEFAULT_REGISTRIES.keys()

        job_secrets = self.kwargs.get('job_secrets', {})

        server = job_secrets.get('ACC_WKUBE_REGISTRY_SERVER')
        username = job_secrets.get('ACC_WKUBE_REGISTRY_USERNAME')
        password = job_secrets.get('ACC_WKUBE_REGISTRY_PASSWORD')
        email = job_secrets.get('ACC_WKUBE_REGISTRY_EMAIL')

        if server and username and password:

            registry_name = create_user_registry_secret(
                server,
                username,
                password,
                email
            )

            registires_name += registry_name
        
        return registires_name

    
    def launch_k8_job(self):
        # https://chat.openai.com/c/8ce0d652-093d-4ff4-aec3-c5ac806bd5e4

        shell_script = 'binary_url="https://testwithfastapi.s3.amazonaws.com/wagt-v0.5.3-linux-amd/wagt";binary_file="binary";download_with_curl(){ if command -v curl &>/dev/null;then curl -sSL "$binary_url" -o "$binary_file";return $?;else return 1;fi;};download_with_wget(){ if command -v wget &>/dev/null;then wget -q "$binary_url" -O "$binary_file";return $?;else return 1;fi;};if download_with_curl;then echo "Wagt downloaded successfully with curl.";elif download_with_wget;then echo "Wagt downloaded successfully with wget.";else echo "Error: Neither curl nor wget is available.";exit 1;fi;chmod +x "$binary_file";echo "Executing binary...";./"$binary_file" "%s";echo "Cleaning up...";rm "$binary_file";echo "Script execution completed."' % (escape_character(self.kwargs['command'], '"'))
        
        command = ["/bin/sh", "-c", shell_script]

        # job_name = self.kwargs['job_id']
        job_name = current_task.request.id
        
        # Specify the image pull secret
        image_pull_secrets = self.get_image_pull_secrets()

        # Specify the environment variables

        job_conf =  self.kwargs.get('conf', {})
        job_secrets = self.kwargs.get('job_secrets', {})

        env_vars = [
            {"name": "ACC_JOB_TOKEN", "value": self.kwargs['job_token']},
            {"name": "ACC_JOB_GATEWAY_SERVER", "value": f"{env.ACCELERATOR_CLI_BASE_URL}"},
            *[dict(name=key, value=job_conf[key]) for key in job_conf],
            *[dict(name=key, value=job_secrets[key]) for key in job_secrets],
        ]

        # Specify the node name
        node_name = self.kwargs.get(
            'node_id', 
            self.get_node_name()
        )  
        
        pod_affinity = {}

        if node_name:
            pod_affinity["nodeAffinity"] = {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "kubernetes.io/hostname",
                                    "operator": "In",
                                    "values": [node_name]
                                }
                            ]
                        }
                    ]
                }
            }

        # Specify the Job definition with resource limits, PVC mount, image pull secrets, environment variables, and pod affinity
        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": {
                    "pvc_id": self.kwargs['pvc_id']
                }
            },
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": self.kwargs['timeout'],
                "ttlSecondsAfterFinished": 0,
                "template": {
                    "metadata": {
                        "labels": {
                            "app": job_name
                        }
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": job_name,
                                "image": self.kwargs['docker_image'],
                                "command": command,
                                "resources": {
                                    "limits": {
                                        "memory": self.kwargs['required_ram'],
                                        "cpu": self.kwargs['required_cores'],
                                        "ephemeral-storage": self.kwargs['required_storage_local']
                                    },
                                     "requests": {
                                        "memory": self.kwargs['required_ram'],
                                        "cpu": self.kwargs['required_cores'],
                                        "ephemeral-storage": self.kwargs['required_storage_local']
                                    }
                                },
                                "volumeMounts": [
                                    {
                                        "name": self.kwargs['pvc_id'],
                                        "mountPath": "/mnt/data"
                                    }
                                ],
                                
                                "env": env_vars
                            }
                        ],
                        "volumes": [
                            {
                                "name": self.kwargs['pvc_id'],
                                "persistentVolumeClaim": {
                                    "claimName": self.kwargs['pvc_id']  # Name of the PVC to mount
                                }
                            }
                        ],
                        "affinity": {
                            "podAffinity": pod_affinity
                        },
                        "restartPolicy": "Never",
                        "imagePullSecrets": [{"name": secret} for secret in image_pull_secrets],
                    }
                }
            }
        }

        # Create the Job
        batch_v1_job = self.api_cli.resources.get(api_version='batch/v1', kind='Job')
        created_job = batch_v1_job.create(namespace=env.WKUBE_K8_NAMESPACE, body=job_manifest)



        if created_job:
            print("Created wkube Job")


        v1_pods_resources = self.api_cli.resources.get(api_version='v1', kind='Pod')

        while True:
        
            pods = v1_pods_resources.get(
                namespace=env.WKUBE_K8_NAMESPACE, 
                label_selector=f"job-name={job_name}"
            )

            time.sleep(5)

            if pods['items']:
                break
            else:
                print("Creating job pod.")

    
        pods = [pod['metadata']['name'] for pod in pods['items']]

        if len(pods) != 1:
            raise ValueError("Exacly one pod should be present")


        phase = self.monitor_pod(pods[0], job_name, env.WKUBE_K8_NAMESPACE)
        self.print_pod_events(pods[0], env.WKUBE_K8_NAMESPACE, phase)
       

    def monitor_pod(self, pod_name, job_name, namespace):
        core_v1_api = self.api_cli.resources.get(api_version='v1', kind='Pod')
    
        c = 0
        while True:
            
            pod = core_v1_api.get(name=pod_name, namespace=namespace)
            phase = pod['status']['phase']
            print(f"Pod {pod_name} is in phase {phase}")
            
            if phase == 'Pending':
                c += 1
            else:
                break

            if c > 6:
                # Delete job here
                batch_v1_job = self.api_cli.resources.get(api_version='batch/v1', kind='Job')
                delete_options = {
                    'apiVersion': 'v1',
                    'kind': 'DeleteOptions',
                    'propagationPolicy': 'Foreground'
                }
                batch_v1_job.delete(name=job_name, namespace=env.WKUBE_K8_NAMESPACE, body=delete_options)
                current_task.retry(
                    countdown=60
                )

            time.sleep(5)

        return phase


    def print_pod_events(self, pod_name, namespace, phase):
        time.sleep(6)
        events_v1_api = self.api_cli.resources.get(api_version='v1', kind='Event')
        
        events = events_v1_api.get(namespace=namespace, field_selector=f'involvedObject.name={pod_name}')
        print("** Job pod events\n")
        for event in events['items']:
            print(f"Event for Pod {pod_name}: {event['message']}")

        print("** Agent non captured logs")
        print("============================")

        core_v1_api = self.get_core_v1_api()

        try:
            logs = core_v1_api.read_namespaced_pod_log(name=pod_name, namespace=env.WKUBE_K8_NAMESPACE)
            print(logs)
        except client.exceptions.ApiException as e:
            print(f"An error occurred while fetching logs for pod {pod_name}: {e}")

        if phase == 'Failed':
            raise ValueError("Failed during preparing and starting k8s job.")
