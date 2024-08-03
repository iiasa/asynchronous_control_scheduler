import json
import base64
import hashlib

from acc_worker.configs.Environment import get_environment_variables

from kubernetes import client, config, dynamic
from kubernetes.client import api_client

from kubernetes.dynamic import exceptions as k8exceptions

env = get_environment_variables()


def get_dcli():
    
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
    
    dcli = dynamic.DynamicClient(
        api_client.ApiClient()
    )

    return dcli

