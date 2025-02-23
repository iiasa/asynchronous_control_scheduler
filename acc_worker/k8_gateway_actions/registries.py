import json
import base64
import hashlib

from acc_worker.configs.Environment import get_environment_variables

from acc_worker.k8_gateway_actions.commons import get_dcli

from kubernetes.dynamic import exceptions as k8exceptions

env = get_environment_variables()

DEFAULT_REGISTRIES = {
    "jobstore": {
        "server": env.IMAGE_REGISTRY_URL,
        "username": env.IMAGE_REGISTRY_USER,
        "password": env.IMAGE_REGISTRY_PASSWORD,
        "email": ""
    }
}

def create_default_registry_secret_resource():
    dcli = get_dcli()

    for default_secret_name in DEFAULT_REGISTRIES.keys():
        b64_secret = create_b64_default_secret_json(
            DEFAULT_REGISTRIES[default_secret_name]['server'],
            DEFAULT_REGISTRIES[default_secret_name]['username'],
            DEFAULT_REGISTRIES[default_secret_name]['password'],
            DEFAULT_REGISTRIES[default_secret_name]['email'],
        )

        create_secret(dcli, default_secret_name, b64_secret)


def create_user_registry_secret(server, username, password, email=None):
    dcli = get_dcli()

    
    b64_secret = create_b64_default_secret_json(
        server, username, password, email
    )

    email = email or ''

    secret_name = hashlib.md5(f"{server}-{username}-{password}-{email}".encode()).hexdigest()

    create_secret(dcli, secret_name, b64_secret)

    return secret_name


def create_b64_default_secret_json(server, username, password, email=None):
    docker_config = {
        "auths": {
            server: {
                "username": username,
                "password": password
            }
        }
    }

    if email:
        docker_config["auths"][server]["email"] = email

    docker_config_json = json.dumps(docker_config)
    
    encoded_docker_config_json = base64.b64encode(docker_config_json.encode()).decode()
    
    return encoded_docker_config_json

def create_secret(dynamic_client, secret_name, docker_config_json):
    # Define the Secret
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": secret_name
        },
        "type": "kubernetes.io/dockerconfigjson",
        "data": {
            ".dockerconfigjson": docker_config_json
        }
    }

    # Create the Secret
    v1_secrets = dynamic_client.resources.get(api_version='v1', kind='Secret')
    try:
        v1_secrets.create(body=secret, namespace=env.WKUBE_K8_NAMESPACE)
    except k8exceptions.ConflictError:
        pass
    print(f"Secret '{secret_name}' created.")