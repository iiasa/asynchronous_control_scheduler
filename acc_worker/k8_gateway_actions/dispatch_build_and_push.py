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
from functools import cached_property
from pathlib import Path
from celery import current_task
from minio import Minio
from datetime import datetime, timedelta
from kubernetes import client, config, dynamic
from kubernetes.client import api_client
from celery import current_task

import threading
from kubernetes import watch, client
import time

from acc_worker.k8_gateway_actions.periodic_tasks import delete_pvc

from kubernetes.dynamic.exceptions import NotFoundError, ConflictError

from accli import AjobCliService

from acc_worker.configs.Environment import get_environment_variables

from .registries import DEFAULT_REGISTRIES, create_user_registry_secret

env = get_environment_variables()

# FOLDER_JOB_REPO_URL = 'https://github.com/IIASA-Accelerator/wkube-job.git'


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
    R4_4 = 'R4_4'
    GAMS40_1__R4_4 = 'GAMS40_1__R4_4'
    WINE64__CONSOLE = 'WINE64__CONSOLE'

def exec_command(command, raise_exception=True, cwd=None):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)

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
            force_build=False,
            user_id=None,
            job_name=None,
            internal_build=False
        ):
        self.internal_build = internal_build
        self.git_repo = git_repo.lower()
        self.version = version
        self.dockerfile = dockerfile
        self.base_stack = base_stack
        self.force_build = force_build
        self.job_secrets = job_secrets
        self.user_id = user_id
        self.job_name = job_name # This is queue task id

        self.set_image_building_site()
        self.set_dockerfile_path()

        if self.tag_exists() and (not self.force_build):
            print(
                f"WKube Builder: Skipping image build as image for given repo and"
                f" tag already exists, force update is turned off."
            )
            return self.image_tag

        if self.internal_build:
            # This is the actual build process running inside the K8s builder Job
            try:
                print(f"Internal build started for {self.git_repo}:{self.version}", flush=True)
                print("Preparing files...", flush=True)
                self.prepare_files()
                print("Starting build...", flush=True)
                self.build()
                print("Pushing to registry...", flush=True)
                self.push_to_registry()
                print("Cleaning up...", flush=True)
                self.clean_up()
                print("Internal build completed successfully.", flush=True)
            except Exception as e:
                print(f"Internal build failed: {e}", flush=True)
                import traceback
                traceback.print_exc()
                raise
            finally:
                self.clear_site()
            return self.image_tag

        return self.dispatch_k8s_job()

    def _init_k8s_config(self):
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

    def get_api_cli(self):
        self._init_k8s_config()
        return dynamic.DynamicClient(api_client.ApiClient())

    def get_core_v1_api(self):
        self._init_k8s_config()
        return client.CoreV1Api()

    @property
    def k8s_job_name(self):
        """Generate a K8s-compliant Job name from the image tag."""
        # K8s job name must be DNS-compliant (alphanumeric, dots, dashes, < 63 chars)
        name = self.image_tag.split('/')[-1] # use the image name part
        name = name.lower()
        name = re.sub(r'[^a-z0-9\-]', '-', name)
        name = re.sub(r'-+', '-', name).strip('-')
        return f"build-{name}"[:63].rstrip('-')

    def get_image_pull_secrets(self):
        return list(DEFAULT_REGISTRIES.keys())

    def dispatch_k8s_job(self):
        api_cli = self.get_api_cli()
        batch_v1_job = api_cli.resources.get(api_version='batch/v1', kind='Job')
        
        job_name = self.k8s_job_name
        
        try:
            job = batch_v1_job.get(name=job_name, namespace=env.WKUBE_K8_NAMESPACE)
            
            if getattr(self, 'force_build', False):
                print(f"Force build enabled. Deleting existing job {job_name}...")
                batch_v1_job.delete(name=job_name, namespace=env.WKUBE_K8_NAMESPACE, propagation_policy='Foreground')
                # Wait for deletion
                for _ in range(12): # Wait up to 60s
                    try:
                        batch_v1_job.get(name=job_name, namespace=env.WKUBE_K8_NAMESPACE)
                        time.sleep(5)
                    except NotFoundError:
                        break
            else:
                status = job.status
                
                if status.active:
                    print(f"Job {job_name} is already active. Streaming logs...")
                    return self.monitor_and_stream_logs(job_name)
                
                if status.succeeded:
                    print(f"Job {job_name} succeeded previously. Verifying registry...")
                    if self.tag_exists():
                        return self.image_tag
                    else:
                        print(f"Image not found in registry despite successful job. Deleting stale job and restarting...")
                        batch_v1_job.delete(name=job_name, namespace=env.WKUBE_K8_NAMESPACE, propagation_policy='Foreground')
                        # Wait for deletion
                        for _ in range(12): # Wait up to 60s
                            try:
                                batch_v1_job.get(name=job_name, namespace=env.WKUBE_K8_NAMESPACE)
                                time.sleep(5)
                            except NotFoundError:
                                break
                
                elif status.failed:
                    print(f"Job {job_name} failed previously. Streaming logs and exiting...")
                    self.monitor_and_stream_logs(job_name)
                    raise ValueError(f"Build job {job_name} failed.")

        except NotFoundError:
            pass

        # Create new Job
        return self.create_and_monitor_job(batch_v1_job, job_name)

    def create_and_monitor_job(self, batch_v1_job, job_name):
        # Prepare env vars - inherit all environment from current process
        env_vars = []
        for key, value in os.environ.items():
            # Skip some k8s-specific env vars that might interfere
            if key in ['KUBERNETES_SERVICE_HOST', 'KUBERNETES_SERVICE_PORT', 'KUBERNETES_PORT', 'KUBERNETES_PORT_443_TCP', 'KUBERNETES_PORT_443_TCP_ADDR', 'KUBERNETES_PORT_443_TCP_PORT', 'KUBERNETES_PORT_443_TCP_PROTO']:
                continue
            env_vars.append({"name": key, "value": str(value)})

        # Explicitly add current OCIImageBuilder parameters as env vars to the builder
        builder_env = {
            "BUILDER_GIT_REPO": self.git_repo,
            "BUILDER_VERSION": self.version,
            "BUILDER_DOCKERFILE": self.dockerfile or "",
            "BUILDER_BASE_STACK": self.base_stack or "",
            "BUILDER_FORCE_BUILD": str(self.force_build),
            "BUILDER_USER_ID": str(self.user_id or ""),
            "BUILDER_JOB_NAME": self.job_name or "",
            "BUILDER_TAG": self.image_tag,
            "BUILDER_JOB_SECRETS_B64": base64.b64encode(json.dumps(self.job_secrets).encode()).decode(),
            "PYTHONUNBUFFERED": "1"
        }
        for k, v in builder_env.items():
            env_vars.append({"name": k, "value": v})

        image_pull_secrets = self.get_image_pull_secrets()

        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name, "namespace": env.WKUBE_K8_NAMESPACE},
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "spec": {
                        "imagePullSecrets": [{"name": secret} for secret in image_pull_secrets],
                        "containers": [{
                            "name": "builder",
                            "image": env.OCI_BUILDER_IMAGE,
                            "env": env_vars,
                            "resources": {
                                "requests": {
                                    "cpu": "1",
                                    "memory": "2Gi"
                                },
                                "limits": {
                                    "cpu": "4",
                                    "memory": "8Gi"
                                }
                            },
                            "volumeMounts": [{
                                "name": "container-storage",
                                "mountPath": "/home/ubuntu/.local/share/containers/storage"
                            }],
                            "command": [
                                "/bin/sh", "-c", 
                                "echo '--- Container Diagnostics ---'; "
                                "id; "
                                "ls -ld /home/ubuntu/.local/share/containers/storage; "
                                "echo '--- Starting Python Builder ---'; "
                                "python3 -u -c \"\n"
                                "import os, base64, json, sys\n"
                                "print('Builder container process started...', flush=True)\n"
                                "try:\n"
                                "    from acc_worker.k8_gateway_actions.dispatch_build_and_push import OCIImageBuilder\n"
                                "    job_secrets = json.loads(base64.b64decode(os.environ['BUILDER_JOB_SECRETS_B64']).decode())\n"
                                "    builder = OCIImageBuilder()\n"
                                "    builder(\n"
                                "        git_repo=os.environ['BUILDER_GIT_REPO'],\n"
                                "        version=os.environ['BUILDER_VERSION'],\n"
                                "        job_secrets=job_secrets,\n"
                                "        dockerfile=os.environ['BUILDER_DOCKERFILE'] or None,\n"
                                "        base_stack=os.environ['BUILDER_BASE_STACK'] or None,\n"
                                "        force_build=os.environ['BUILDER_FORCE_BUILD'] == 'True',\n"
                                "        user_id=os.environ['BUILDER_USER_ID'] or None,\n"
                                "        job_name=os.environ['BUILDER_JOB_NAME'] or None,\n"
                                "        internal_build=True\n"
                                "    )\n"
                                "except Exception as e:\n"
                                "    print(f'FATAL ERROR in builder: {e}', flush=True)\n"
                                "    import traceback; traceback.print_exc()\n"
                                "    sys.exit(1)\n"
                                "\"\n"
                            ],
                            "securityContext": {"privileged": True} # Needed for buildah in many k8s setups
                        }],
                        "volumes": [{
                            "name": "container-storage",
                            "ephemeral": {
                                "volumeClaimTemplate": {
                                    "spec": {
                                        "storageClassName": env.WKUBE_WORKFLOW_STORAGE_CLASS,
                                        "accessModes": ["ReadWriteOnce"],
                                        "resources": {
                                            "requests": {
                                                "storage": "10Gi"
                                            }
                                        }
                                    }
                                }
                            }
                        }],
                        "restartPolicy": "Never"
                    }
                }
            }
        }

        try:
            batch_v1_job.create(namespace=env.WKUBE_K8_NAMESPACE, body=job_manifest)
            print(f"Created build job {job_name}")
        except ConflictError:
            print(f"Job {job_name} was created by another process. Attaching...")
        
        return self.monitor_and_stream_logs(job_name)

    def monitor_and_stream_logs(self, job_name):
        core_v1 = self.get_core_v1_api()
        api_cli = self.get_api_cli()
        
        # Wait for Pod to be created and find it
        pod_name = None
        for _ in range(20): # 60 seconds timeout
            pods = core_v1.list_namespaced_pod(namespace=env.WKUBE_K8_NAMESPACE, label_selector=f"job-name={job_name}")
            if pods.items:
                pod_name = pods.items[0].metadata.name
                break
            time.sleep(3)
        
        if not pod_name:
            raise ValueError(f"Could not find pod for job {job_name}")

        # Wait for container to be ready or at least not pending
        for i in range(30):
            pod = core_v1.read_namespaced_pod(name=pod_name, namespace=env.WKUBE_K8_NAMESPACE)
            phase = pod.status.phase
            if phase != 'Pending':
                print(f"Pod {pod_name} moved to phase {phase}.")
                break
            
            # Check for container issues
            container_statuses = pod.status.container_statuses
            if container_statuses:
                state = container_statuses[0].state
                if state.waiting:
                    print(f"Waiting for pod {pod_name} (Reason: {state.waiting.reason}, Message: {state.waiting.message})")
                elif state.terminated:
                    print(f"Pod {pod_name} terminated immediately (Reason: {state.terminated.reason}, Exit Code: {state.terminated.exit_code})")
                    break
            else:
                print(f"Waiting for pod {pod_name} to start (current phase: {phase})...")
            
            time.sleep(3)

        # Stream logs
        try:
            w = watch.Watch()
            for line in w.stream(core_v1.read_namespaced_pod_log, name=pod_name, namespace=env.WKUBE_K8_NAMESPACE, follow=True):
                print(f"[K8S-BUILD] {line}")
        except Exception as e:
            print(f"Log streaming interrupted: {e}")

        # Final check
        while True:
            job = api_cli.resources.get(api_version='batch/v1', kind='Job').get(name=job_name, namespace=env.WKUBE_K8_NAMESPACE)
            if job.status.succeeded:
                if self.tag_exists():
                    return self.image_tag
                raise ValueError(f"Job {job_name} succeeded but image is missing in registry.")
            if job.status.failed:
                raise ValueError(f"Build job {job_name} failed.")
            time.sleep(5)

    @cached_property
    def commit_hash(self):
        if self.git_repo.startswith("s3accjobstore://"):
            return None

        try:
            # git ls-remote https://username:password@git.example.com/your/repo.git main | awk '{print substr($1, 1, 7)}'
            command = [
                "git", "ls-remote",
                f"{self.git_repo}",
                f"{self.version}"
            ]
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate()
            if process.returncode != 0:
                raise ValueError(f"Failed to get commit hash: {error.decode().strip()}")
            commit_hash = output.decode().strip().split()[0]
            return commit_hash[:7]
        except Exception as e:
            # The fallback will happen if the version is itself a commit hash
            print(f"Couldn't get commit hash: {str(e)}")
            return self.version

        
    
    def set_image_building_site(self):
        """ The logic here is a best way to structure folder to maximize cacheing
        
        >> When the job is dispatched from remote as a zip folder then the cacheing key is 
        based on job name.

        >> When the job is dispatched from git repo then the cacheing key is based on git repo url
        and branch or commit.
          >> Git commit with different commit hash will always trigger fresh build
          >> With same branch and job name the cacheing key will be same and the image build layers can be reused
        """

        hased_job_name = uuid.uuid5(uuid.NAMESPACE_DNS, self.job_name).hex[:7]

        # if self.git_repo.startswith("s3accjobstore://"):
        #     self.IMAGE_BUILDING_SITE = f"{self.IMAGE_BUILDING_SITE}/{self.user_id}/{hased_job_name}"
        # else:

        #     normalized_repo_url_hash = uuid.uuid5(uuid.NAMESPACE_DNS, self.normalized_repo_url).hex[:7]
        #     self.IMAGE_BUILDING_SITE = f"{self.IMAGE_BUILDING_SITE}/{self.user_id}/{hased_job_name}/{normalized_repo_url_hash}/{self.version}"

        self.IMAGE_BUILDING_SITE = f"{self.IMAGE_BUILDING_SITE}/{uuid.uuid4().hex}"

    def set_dockerfile_path(self):
        # Default dockerfile path
        self.dockerfile_path = f"{self.IMAGE_BUILDING_SITE}/Dockerfile"

        # Step 2. Either dockerfile should be present or base_stack should be choosen
        if not (self.dockerfile or self.base_stack):
            raise ValueError("Either dockerfile of base_stack should be present with the job")
        
        if self.dockerfile:
            dockerfile_path = Path(f"{self.IMAGE_BUILDING_SITE}/{self.dockerfile}")
            self.dockerfile_path = str(dockerfile_path)
        else:
            self.create_dockerfile_for_basestack()
            
    
    def tag_exists(self):
        command = [
            "skopeo", "inspect", "--tls-verify=false", "--creds", 
            f"{env.IMAGE_REGISTRY_USER}:{env.IMAGE_REGISTRY_PASSWORD}",
            f"docker://{self.image_tag}"
        ]

        exit_code = exec_command(command, raise_exception=False)

        return exit_code == 0
    
    def get_git_pull_url(self):
        username = self.job_secrets.get('ACC_WKUBE_GIT_USER', None)
        password = self.job_secrets.get('ACC_WKUBE_GIT_PASSWORD', None)

        if username and password:
            return f"https://{username}:{password}@{self.git_repo.split('https://')[1]}"
        else:
            return self.git_repo
        # else:
        #     if self.git_repo == env.FOLDER_JOB_REPO_URL:
        #         raise ValueError(f"ACC_WKUBE_GIT_USER and ACC_WKUBE_GIT_PASSWORD job_secrets are required and supposed to be set by accelerator gateway.")

    def pull_files_from_git(self):

        # Delete the folder if it exists
        if os.path.exists(self.IMAGE_BUILDING_SITE):
            shutil.rmtree(self.IMAGE_BUILDING_SITE)
        os.makedirs(self.IMAGE_BUILDING_SITE, exist_ok=True)

        clone_command = [
            "git", "clone",
            "--depth", "1",
            "--no-checkout",  # Avoid checking out submodules
            "--branch", self.version,
            f"{self.get_git_pull_url()}",
            self.IMAGE_BUILDING_SITE
        ]
        exec_command(clone_command)

        # Check out the main working tree explicitly
        exec_command(["git", "reset", "--hard", "HEAD"], cwd=self.IMAGE_BUILDING_SITE)

        # Step 3: Normalize timestamps to enable buildah cache
        exec_command([
            "find", ".", "-type", "f", "-exec", "touch", "-d", "2023-01-01T00:00:00Z", "{}", "+"
        ], cwd=self.IMAGE_BUILDING_SITE)

        # Step 4: Add or update .dockerignore to exclude .git and .gitmodules
        dockerignore_path = os.path.join(self.IMAGE_BUILDING_SITE, ".dockerignore")
        ignored_entries = {".git", ".gitmodules"}

        existing_entries = set()
        if os.path.exists(dockerignore_path):
            with open(dockerignore_path, "r") as f:
                existing_entries = {line.strip() for line in f if line.strip()}

        # Merge with what's already there
        updated_entries = existing_entries.union(ignored_entries)

        # Write back updated .dockerignore
        with open(dockerignore_path, "w") as f:
            for entry in sorted(updated_entries):
                f.write(f"{entry}\n")

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

        self.clear_site()

        if self.git_repo.startswith("s3accjobstore://"):
            self.pull_files_from_job_store()
        
        else:
            self.pull_files_from_git()

        if not Path(self.dockerfile_path).is_file():
            raise ValueError(f"{self.dockerfile_path} does not exists")


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

    
    @cached_property
    def get_dockerfile_hash(self):
        """Get the hash of the dockerfile in 7 characters
        """

        if self.dockerfile:
            hash_value = uuid.uuid5(uuid.NAMESPACE_DNS, self.dockerfile).hex[:7]
        else:
            hash_value = uuid.uuid5(
                uuid.NAMESPACE_DNS, 
                f"{self.PREDEFINED_STACKS_FOLDER}/Dockerfile.{self.base_stack}"
            ).hex[:7]

        return hash_value

    @cached_property
    def normalized_repo_url(self):
        """Get the repo url from the git repo
        """

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

        return url
        
    @cached_property
    def image_tag(self):
        
        # Addition of get_dockerfile_hash is to prevent collision beteween same source with different base stacks
        return f"{env.IMAGE_REGISTRY_URL}/{env.IMAGE_REGISTRY_TAG_PREFIX}{self.normalized_repo_url}-{self.get_dockerfile_hash}:{self.commit_hash}"

    
    def build(self):
        # buildah login --username myregistry --password myregistrypassword registry:8443

        command = [
            "sudo", "buildah", "bud",
            "--tls-verify=false",
            "--creds", f"{env.IMAGE_REGISTRY_USER}:{env.IMAGE_REGISTRY_PASSWORD}",
            "--layers",
            "--cache-from", f"{env.IMAGE_REGISTRY_URL}/{env.IMAGE_REGISTRY_TAG_PREFIX}project-cache",
            "--cache-to", f"{env.IMAGE_REGISTRY_URL}/{env.IMAGE_REGISTRY_TAG_PREFIX}project-cache",
            "--isolation", "chroot",
            "-t", self.image_tag,
            "-f", self.dockerfile_path,
            self.IMAGE_BUILDING_SITE
        ]

        exec_command(command)

    def push_to_registry(self):
        # buildah login --username myregistry --password myregistrypassword registry:8443

        push_command = [
            "sudo", "buildah", "push",
            "--tls-verify=false",
            "--creds", f"{env.IMAGE_REGISTRY_USER}:{env.IMAGE_REGISTRY_PASSWORD}",
            "--remove-signatures",
            self.image_tag
        ]

        exec_command(push_command)

    def clean_up(self):
        """
        Clean up buildah containers and images.
        """
        # First remove all build containers (this prevents 'image used by container' errors)
        remove_containers_command = [
            "sudo", "buildah", "rm", "--all"
        ]
        exec_command(remove_containers_command)

        # Now remove the built image
        remove_built_image_command = [
            "sudo", "buildah", "rmi", self.image_tag
        ]
        exec_command(remove_built_image_command)

        # Optional: prune dangling images
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

        self.volumes = []

    def get_core_v1_api(self):
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
            force_build=self.kwargs['force_build'],
            user_id=self.kwargs['user_id'],
            job_name=self.kwargs['job_name'],
        )
    
    def get_workflow_pvc_details(self):
        try:
            pvc = self.api_cli.resources.get(api_version='v1', kind='PersistentVolumeClaim').get(
                namespace=env.WKUBE_K8_NAMESPACE, name=self.kwargs['pvc_id'])
            if pvc:
                print(f"PVC {self.kwargs['pvc_id']} exists in namespace {env.WKUBE_K8_NAMESPACE}.")
                return pvc
            else:
                print(f"PVC {self.kwargs['pvc_id']} does not exist in namespace {env.WKUBE_K8_NAMESPACE}.")
                return None
        except NotFoundError:
            return None

    def get_graph_pvc_name(self):
        return f"graph-pvc-{self.kwargs['root_job_id']}"

    def get_graph_pvc_details(self):
        try:
            pvc = self.api_cli.resources.get(api_version='v1', kind='PersistentVolumeClaim').get(
                namespace=env.WKUBE_K8_NAMESPACE, name=self.get_graph_pvc_name())
            if pvc:
                print(f"Graph PVC {self.get_graph_pvc_name()} exists in namespace {env.WKUBE_K8_NAMESPACE}.")
                return pvc
            else:
                print(f"Graph PVC {self.get_graph_pvc_name()} does not exist in namespace {env.WKUBE_K8_NAMESPACE}.")
                return None
        except NotFoundError:
            return None
     

    def get_or_create_graph_pvc(self):
       
        pvc = self.get_graph_pvc_details()

        if pvc:

            if not self.kwargs['is_first_graph_job']:
                return

            # Deletes pvc when it is first graph job
            delete_pvc(self.get_graph_pvc_name())
            while True:
                pvc = self.get_graph_pvc_details()
                if not pvc:  # PVC is fully deleted
                    break
                print(f"Waiting for existing graph PVC '{self.get_graph_pvc_name()}' to be fully deleted...")
                time.sleep(5)


        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": self.get_graph_pvc_name()
            },
            "spec": {
                "storageClassName": env.WKUBE_GRAPH_STORAGE_CLASS,
                "accessModes": [
                    "ReadWriteMany"
                ],
                "resources": {
                    "requests": {
                        "storage": self.kwargs.get('required_storage_graph')
                    }
                }
            }
        }


        # Create the PVC with volumeBindingMode: "WaitForFirstConsumer"
        v1_pvc = self.api_cli.resources.get(api_version='v1', kind='PersistentVolumeClaim')
        created_pvc = v1_pvc.create(namespace=env.WKUBE_K8_NAMESPACE, body=pvc_manifest)

        print("Created graph PVC:", created_pvc)

        return
    
    
    
    def get_or_create_workflow_pvc(self):
       
        pvc = self.get_workflow_pvc_details()

        if pvc:

            if not self.kwargs['is_first_pipeline_job']:
                return
            
            delete_pvc(self.kwargs['pvc_id'])
            while True:
                pvc = self.get_workflow_pvc_details()
                if not pvc:  # PVC is fully deleted
                    break
                print(f"Waiting for existing workflow PVC '{self.kwargs['pvc_id']}' to be fully deleted...")
                time.sleep(5)


        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": self.kwargs['pvc_id']
            },
            "spec": {
                "storageClassName": env.WKUBE_WORKFLOW_STORAGE_CLASS,
                "accessModes": [
                    "ReadWriteOnce"
                ],
                "resources": {
                    "requests": {
                        "storage": self.kwargs.get('required_storage_workflow')
                    }
                }
            }
        }


        # Create the PVC with volumeBindingMode: "WaitForFirstConsumer"
        v1_pvc = self.api_cli.resources.get(api_version='v1', kind='PersistentVolumeClaim')
        created_pvc = v1_pvc.create(namespace=env.WKUBE_K8_NAMESPACE, body=pvc_manifest)

        print("Created pipeline PVC:", created_pvc)

        return

    # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/CoreV1Api.md
    def get_node_name(self):
        if self.kwargs['is_first_pipeline_job']:
            return self.kwargs.get(
            'node_id')
        else:
            label_selector = f"pvc_id={self.kwargs['pvc_id']}"

            pod_resource = self.api_cli.resources.get(api_version='v1', kind='Pod')
            pods = pod_resource.get(namespace=env.WKUBE_K8_NAMESPACE, label_selector=label_selector)


            # If any pod matches, return the node name of the first one
            if pods.items:
                print(f"______________________FOLLOW UP NODE NAME {pods.items[0].spec.node_name}")
                return pods.items[0].spec.node_name

    def __call__(self):
        self.kwargs['docker_image'] = self.get_or_create_job_image()

        graph_storage = self.kwargs.get('required_storage_graph')
        if graph_storage:
            self.get_or_create_graph_pvc()
            self.volumes.append(
                dict(
                    name=self.get_graph_pvc_name(),
                    mount_point='/mnt/graph'
                )
            )

        if self.kwargs['build_only_task']:
            return

        
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


        main_container_shell_script = '''
            binary_file="/mnt/agent/wagt"

            # Define termination handler
            cleanup() {
                echo "SIGTERM received. Stopping wagt..."
                if [ -n "$BINARY_PID" ]; then
                    kill -TERM "$BINARY_PID"
                    wait "$BINARY_PID"
                    echo "Wagt process stopped."
                fi
                CLEANUP_TRIGGERED=true
            }

            # Trap SIGTERM
            trap cleanup SIGTERM

            CLEANUP_TRIGGERED=false

            echo "Executing binary..."
            "$binary_file" "%s" &
            BINARY_PID=$!

            # Wait for binary to finish or be terminated
            wait "$BINARY_PID"
            EXIT_CODE=$?

            echo "Wagt execution completed with exit code $EXIT_CODE."

            # Always sleep for 30 seconds regardless of how it ended
            echo "Sleeping for 30 seconds..."
            sleep 30

            # Exit with the binary's exit code if no SIGTERM, or 0 if SIGTERM occurred
            if [ "$CLEANUP_TRIGGERED" = true ]; then
                exit 0
            else
                exit "$EXIT_CODE"
            fi

        ''' % (escape_character(self.kwargs['command'], '"'))


        main_container_command = ["/bin/sh", "-c", main_container_shell_script]
        
        # shell_script = 'binary_url="https://testwithfastapi.s3.amazonaws.com/wagt-v0.5.3-linux-amd/wagt";binary_file="binary";download_with_curl(){ if command -v curl &>/dev/null;then curl -sSL "$binary_url" -o "$binary_file";return $?;else return 1;fi;};download_with_wget(){ if command -v wget &>/dev/null;then wget -q "$binary_url" -O "$binary_file";return $?;else return 1;fi;};if download_with_curl;then echo "Wagt downloaded successfully with curl.";elif download_with_wget;then echo "Wagt downloaded successfully with wget.";else echo "Error: Neither curl nor wget is available.";exit 1;fi;chmod +x "$binary_file";echo "Executing binary...";./"$binary_file" "%s";echo "Cleaning up...";rm "$binary_file";echo "Script execution completed."' % (escape_character(self.kwargs['command'], '"'))
        
        # command = ["/bin/sh", "-c", shell_script]
        

        # job_name = self.kwargs['job_id']
        job_name = current_task.request.id
        
        # Specify the image pull secret
        image_pull_secrets = self.get_image_pull_secrets()

        # Specify the environment variables

        job_conf =  self.kwargs.get('conf', {})
        job_secrets = self.kwargs.get('job_secrets', {})

        env_vars = [
            *[dict(name=key, value=str(job_conf[key])) for key in job_conf],
            *[dict(name=key, value=str(job_secrets[key])) for key in job_secrets],
            {"name": "JOB_ID", "value": str(self.kwargs['job_id'])},
            {"name": "POD_ID", "value": str(job_name)},
            {"name": "ROOT_JOB_ID", "value": str(self.kwargs['root_job_id'])},

            {"name": "ACC_JOB_TOKEN", "value": self.kwargs['job_token']},
            {"name": "ACC_JOB_GATEWAY_SERVER", "value": f"{env.ACCELERATOR_CLI_BASE_URL}"},

            # TODO check ssh user 
            {"name": "TUNNEL_GATEWAY_SSH_USER", "value": 'linuxserver.io'},
            {"name": "TUNNEL_GATEWAY_SSH_SERVER", "value": 'hpg1021'},
            {"name": "TUNNEL_GATEWAY_SSH_PORT", "value": '30188'},
            {"name": "TUNNEL_GATEWAY_DOMAIN", "value": 'wkube.iiasa.ac.at'},
            {"name": "TUNNEL_GATEWAY_PORT", "value": ''},
            {"name": "TUNNEL_GATEWAY_SSH_PRIVATE_KEY_BASE64", "value": f"{env.TUNNEL_GATEWAY_SSH_PRIVATE_KEY_BASE64}"},
            {
                "name": "CLUSTER_NODE_NAME",
                "valueFrom": {
                    "fieldRef": {
                        "fieldPath": "spec.nodeName"
                    }
                }
            }
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

        

        workflow_storage = self.kwargs.get('required_storage_workflow')
        
        if workflow_storage:
            self.get_or_create_workflow_pvc()
            self.volumes.append(
                dict(
                    name=self.kwargs['pvc_id'],
                    mount_point='/mnt/pipe'
                )
            )

        # Specify the Job definition with resource limits, PVC mount, image pull secrets, environment variables, and pod affinity
        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": {
                    "pvc_id": self.kwargs['pvc_id'],
                    "job_name": job_name
                }
            },
            "spec": {
                "backoffLimit": 0,
                "ttlSecondsAfterFinished": 60*5,
                "template": {
                    "metadata": {
                        "labels": {
                            "app": job_name
                        }
                    },
                    "spec": {
                        # "activeDeadlineSeconds": self.kwargs['timeout'],
                        "hostUsers": False,   # TODO make is configurable @wrufesh
                        "terminationGracePeriodSeconds": 30,
                        "initContainers": [
                            {
                                "name": "wkube-agent-puller",
                                "image": env.WKUBE_AGENT_PULLER,
                                "volumeMounts": [
                                    {
                                        "name": f"{job_name}-agent-volume",
                                        "mountPath": "/mnt/agent"
                                    }
                                ]
                            }
                        ],
                        "securityContext": {
                            "fsGroup": 65534
                        },
                        "containers": [
                            {
                                "name": job_name,
                                "image": self.kwargs['docker_image'],
                                "command": main_container_command,
                                "resources": {
                                    "limits": {
                                        "memory": self.kwargs['required_ram'],
                                        "cpu": float(self.kwargs['required_cores']),
                                        "ephemeral-storage": self.kwargs.get('required_storage_local', 1024*1024*1024 * 2)
                                    },
                                     "requests": {
                                        "memory": self.kwargs['required_ram'],
                                        "cpu": self.kwargs['required_cores'],
                                        "ephemeral-storage": self.kwargs.get('required_storage_local', 1024*1024*1024 * 2)
                                    }
                                },
                               
                                "volumeMounts": [
                                    {
                                        "name": f"{job_name}-agent-volume",
                                        "mountPath": "/mnt/agent"
                                    },
                                    *[{
                                        "name": volume["name"],
                                        "mountPath": volume["mount_point"]
                                    } for volume in self.volumes]
                                ],
                                
                                "env": env_vars
                            }
                        ],
                       
                        "volumes": [
                            {
                                "name": f"{job_name}-agent-volume",
                                "emptyDir": {}
                            },
                            *[{
                               "name": volume["name"],
                                "persistentVolumeClaim": {
                                    "claimName": volume["name"]
                                }
                           } for volume in self.volumes]
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
        
        created_job = None

        while not created_job:

            try:
                print("Creating job")
                created_job = batch_v1_job.create(namespace=env.WKUBE_K8_NAMESPACE, body=job_manifest)
            except ConflictError:
                print("Deleting conflicting job.")
                delete_options = {
                    'apiVersion': 'v1',
                    'kind': 'DeleteOptions',
                    'propagationPolicy': 'Foreground'
                }
                batch_v1_job.delete(name=job_name, namespace=env.WKUBE_K8_NAMESPACE, body=delete_options)
                time.sleep(5)


        if created_job:
            print("Created wkube Job")

        # BELOW CODE IS REQUIRED FOR DEBUGGING

        # v1_pods_resources = self.api_cli.resources.get(api_version='v1', kind='Pod')

        # while True:
        
        #     pods = v1_pods_resources.get(
        #         namespace=env.WKUBE_K8_NAMESPACE, 
        #         label_selector=f"job-name={job_name}"
        #     )

        #     time.sleep(2)

        #     if pods['items']:
        #         break
        #     else:
        #         print("Creating job pod.")

    
        # pods = [pod['metadata']['name'] for pod in pods['items']]

        # if len(pods) != 1:
        #     raise ValueError("Exacly one pod should be present")

        # self.monitor_pod(pods[0], job_name, env.WKUBE_K8_NAMESPACE)
        # self.print_pod_logs(pods[0], env.WKUBE_K8_NAMESPACE)



    def stream_logs_until_event(self, pod_name, namespace, stop_event):
        core_v1_api = self.get_core_v1_api()
        try:
            w = watch.Watch()
            for line in w.stream(
                core_v1_api.read_namespaced_pod_log,
                name=pod_name,
                namespace=namespace,
                follow=True,
                _preload_content=False,
                timestamps=True,
            ):
                print(line.decode("utf-8").rstrip())
                if stop_event.is_set():
                    w.stop()
                    break
        except client.exceptions.ApiException as e:
            print(f"Error while streaming logs: {e}")

    def monitor_status_and_stop_log(self, pod_name, job_name, namespace, stop_event):
        core_v1_api = self.api_cli.resources.get(api_version='v1', kind='Pod')
        while True:
            pod = core_v1_api.get(name=pod_name, namespace=namespace)
            phase = pod['status']['phase']
            print(f"Pod {pod_name} is in phase {phase}")
            
            if phase in ['Running', 'Succeeded']:
                stop_event.set()
                return phase
            
            if phase in ['Failed']:
                stop_event.set()
                # Handle failure
                batch_v1_job = self.api_cli.resources.get(api_version='batch/v1', kind='Job')
                delete_options = {
                    'apiVersion': 'v1',
                    'kind': 'DeleteOptions',
                    'propagationPolicy': 'Foreground'
                }
                batch_v1_job.delete(name=job_name, namespace=namespace, body=delete_options)
                current_task.retry()
            
            time.sleep(3)

    def monitor_pod_with_early_logs(self, pod_name, job_name, namespace):
        stop_event = threading.Event()

        log_thread = threading.Thread(target=self.stream_logs_until_event, args=(pod_name, namespace, stop_event))
        log_thread.start()

        final_phase = self.monitor_status_and_stop_log(pod_name, job_name, namespace, stop_event)

        log_thread.join()
        print(f"Pod {pod_name} reached final phase: {final_phase}")

    



        # v1_pods_resources = self.api_cli.resources.get(api_version='v1', kind='Pod')

        # while True:
        
        #     pods = v1_pods_resources.get(
        #         namespace=env.WKUBE_K8_NAMESPACE, 
        #         label_selector=f"job-name={job_name}"
        #     )

        #     time.sleep(5)

        #     if pods['items']:
        #         break
        #     else:
        #         print("Creating job pod.")

    
        # pods = [pod['metadata']['name'] for pod in pods['items']]

        # if len(pods) != 1:
        #     raise ValueError("Exacly one pod should be present")


        # self.monitor_pod(pods[0], job_name, env.WKUBE_K8_NAMESPACE)
        # self.print_pod_logs(pods[0], env.WKUBE_K8_NAMESPACE)
       

    def monitor_pod(self, pod_name, job_name, namespace):
        core_v1_api = self.api_cli.resources.get(api_version='v1', kind='Pod')
    
        
        while True:
            
            pod = core_v1_api.get(name=pod_name, namespace=namespace)
            phase = pod['status']['phase']
            print(f"Pod {pod_name} is in phase {phase}")
            
            if phase in ['Succeeded', 'Running']:
                break

            if phase in ['Failed']:
                # Delete job here
                batch_v1_job = self.api_cli.resources.get(api_version='batch/v1', kind='Job')
                delete_options = {
                    'apiVersion': 'v1',
                    'kind': 'DeleteOptions',
                    'propagationPolicy': 'Foreground'
                }
                batch_v1_job.delete(name=job_name, namespace=env.WKUBE_K8_NAMESPACE, body=delete_options)
                current_task.retry()

            time.sleep(3)

        return phase


    def print_pod_logs(self, pod_name, namespace):
        logs = None
        count = 0
        while count < 2:  
            core_v1_api = self.get_core_v1_api()
            try:
                logs = core_v1_api.read_namespaced_pod_log(name=pod_name, namespace=env.WKUBE_K8_NAMESPACE)
            except client.exceptions.ApiException as e:
                print(f"An error occurred while fetching logs for pod {pod_name}: {e}")

            time.sleep(3)
            count = count + 1
            

        print("**** Initial logs -- logs not captured by wkube agent ****")
        print(logs)