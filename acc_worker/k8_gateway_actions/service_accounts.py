import json
import base64
import hashlib

from acc_worker.configs.Environment import get_environment_variables

from acc_worker.k8_gateway_actions.commons import get_dcli
from kubernetes.dynamic import exceptions as k8exceptions

env = get_environment_variables()



def create_cluster_role():

    dynamic_client = get_dcli()

    # Define the ClusterRole
    cluster_role_manifest = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {
            "name": "pvc-manager"
        },
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["persistentvolumeclaims"],
                "verbs": [
                    "get", 
                    "list", 
                    "delete"
                ]
            }
        ]
    }

    try:
        # Create the ClusterRole
        api = dynamic_client.resources.get(api_version='rbac.authorization.k8s.io/v1', kind='ClusterRole')
        api.create(body=cluster_role_manifest)
        print("ClusterRole 'pvc-manager' created.")
    except k8exceptions.ConflictError as err:
        print("ClusterRole 'pvc-manager' already exists.")

def create_cluster_role_binding():
    dynamic_client = get_dcli()
    # Define the ClusterRoleBinding
    cluster_role_binding_manifest = {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {
            "name": "default-service-account-pvc-manager"
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "default",
                "namespace": env.WKUBE_K8_NAMESPACE
            }
        ],
        "roleRef": {
            "kind": "ClusterRole",
            "name": "pvc-manager",
            "apiGroup": "rbac.authorization.k8s.io"
        }
    }

    try:
        # Create the ClusterRoleBinding
        api = dynamic_client.resources.get(api_version='rbac.authorization.k8s.io/v1', kind='ClusterRoleBinding')
        api.create(body=cluster_role_binding_manifest)
        print("ClusterRoleBinding 'default-service-account-pvc-manager' created.")
    except k8exceptions.ConflictError as err:
        print("ClusterRoleBinding 'default-service-account-pvc-manager' already exists.")

def add_pvc_role_to_service_account():
    """Add pvc delete role to default service account"""

    create_cluster_role()
    create_cluster_role_binding()

