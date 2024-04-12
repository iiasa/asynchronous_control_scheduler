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

from kubernetes.dynamic.exceptions import NotFoundError

from accli import AjobCliService

from configs.Environment import get_environment_variables

env = get_environment_variables()

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
    PYTHON3_7 = 'PYTHON3_7'
    GAMS_W_R = 'GAMS_W_R'

def exec_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Read and print the output
    for line in process.stdout:
        print(line.decode().strip())

    # Read and print the error
    for line in process.stderr:
        print(line.decode().strip())

    # Optionally, you can wait for the process to finish
    process.wait()

    return process.returncode

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

        exit_code = exec_command(command)

        return exit_code == 0

    def prepare_files(self):
        
        command = [
                "git", "clone", 
                "--depth", "1", 
                "--branch", self.version, 
                f"{self.git_repo}",
                self.IMAGE_BUILDING_SITE
            ]

        exec_command(command)
    
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

        exec_command(command)

    def push_to_registry(self):

        login_command = [
               "buildah", 
               "login", 
               "--username", env.IMAGE_REGISTRY_USER, 
               "--password", env.IMAGE_REGISTRY_PASSWORD,
               env.IMAGE_REGISTRY_URL
            ]

        exec_command(login_command)

        push_command = ["buildah", "push", self.get_image_tag()]

        exec_command(push_command)

    def clean_up(self):
        
        command = [
            "buildah", "rmi", self.get_image_tag()
        ]

        exec_command(command)
    
    def clear_site(self):

        delete_command = [
                "rm", "-rf", self.IMAGE_BUILDING_SITE
            ]

        exec_command(delete_command)

        make_command = [
                "mkdir", self.IMAGE_BUILDING_SITE
            ]

        exec_command(make_command)


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
                    "ReadWriteOnce"
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

    

    def __call__(self):
        self.kwargs['docker_image'] = self.get_or_create_job_image()

        self.get_or_create_cache_pvc()
        self.launch_k8_job()

    
    def launch_k8_job(self):
        # https://chat.openai.com/c/8ce0d652-093d-4ff4-aec3-c5ac806bd5e4

        shell_script = 'binary_url="https://testwithfastapi.s3.amazonaws.com/wagt-v0.2-linux-amd/wagt";binary_file="binary";download_with_curl(){ if command -v curl &>/dev/null;then curl -sSL "$binary_url" -o "$binary_file";return $?;else return 1;fi;};download_with_wget(){ if command -v wget &>/dev/null;then wget -q "$binary_url" -O "$binary_file";return $?;else return 1;fi;};if download_with_curl;then echo "Wagt downloaded successfully with curl.";elif download_with_wget;then echo "Wagt downloaded successfully with wget.";else echo "Error: Neither curl nor wget is available.";exit 1;fi;chmod +x "$binary_file";echo "Executing binary...";./"$binary_file" "%s";echo "Cleaning up...";rm "$binary_file";echo "Script execution completed."' % (escape_character(self.kwargs['command'], '"'))
        
        command = ["/bin/sh", "-c", shell_script]

        job_name = f"{self.kwargs['job_id']}-{self.kwargs['pvc_id']}"
        
        # Specify the image pull secret
        image_pull_secrets = ["iiasaregcred"]  # Replace "my-secret" with the name of your secret

        # Specify the environment variables
        env_vars = [
            {"name": "JOB_TOKEN", "value": self.kwargs['job_token']},
            {"name": "GATEWAY_SERVER", "value": f"{env.ACCELERATOR_CLI_BASE_URL}/"}
        ]

        # Specify the node name
        node_name = self.kwargs.get('node_id', None)  # Replace "my-node" with the name of the node you want to schedule the Job on

        # Specify the pod affinity based on the job label and node name
        # pod_affinity = {
        #     "requiredDuringSchedulingIgnoredDuringExecution": [
        #         {
        #             "labelSelector": {
        #                 "matchExpressions": [
        #                     {
        #                         "key": self.kwargs["pvc_id"],
        #                         "operator": "In",
        #                         "values": [job_name]
        #                     }
        #                 ]
        #             },
        #             "topologyKey": "kubernetes.io/hostname"
        #         }
        #     ]
        # }
        
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
                "name": job_name
            },
            "spec": {
                "activeDeadlineSeconds": self.kwargs['timeout'],
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

        print("Created Job:", created_job)


        # try:
        #     # Create the Pod
        #     api_instance.create_namespaced_pod(namespace="default", body=pod_manifest)
        #     print("Pod created successfully!")
            
        #     # Wait for the pod to be in a non-pending state
        #     while True:
        #         time.sleep(1)
        #         pod = api_instance.read_namespaced_pod(name="test-pod", namespace="default")
        #         if pod.status.phase != "Pending":
        #             break
                
        #     # Check if pod creation encountered any issues
        #     if pod.status.phase == "Failed":
        #         print("Pod creation failed:", pod.status.message)
        #     else:
        #         print("Pod is ready to execute.")
        # except ApiException as e:
        #     print("Exception when calling CoreV1Api->create_namespaced_pod:", e)
