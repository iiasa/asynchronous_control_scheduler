import urllib3
from urllib3.exceptions import HTTPError
from acc_worker.k8_gateway_actions.commons import get_dcli
from acc_worker.configs.Environment import get_environment_variables

env = get_environment_variables()


retries = urllib3.util.Retry(total=10, backoff_factor=1)

http_client = urllib3.poolmanager.PoolManager(
    cert_reqs="CERT_NONE", num_pools=20, retries=retries
)

def delete_pvc(pvc_name):
    dcli = get_dcli()
    pvc_resource = dcli.resources.get(api_version='v1', kind='PersistentVolumeClaim')

    try:
        # Delete the PVC
        pvc_resource.delete(
            name=pvc_name,
            namespace=env.WKUBE_K8_NAMESPACE,
            grace_period_seconds=0
        )
        print(f"PVC '{pvc_name}' in namespace '{env.WKUBE_K8_NAMESPACE}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting PVC '{pvc_name}' in namespace '{env.WKUBE_K8_NAMESPACE}': {e}")

def delete_orphan_pvcs():

    if not env.ACCELERATOR_APP_TOKEN:
        raise ValueError("env.ACCELERATOR_APP_TOKEN is not set.")

    HEADERS = {
        'Content-Type': 'application/json',
        'x-authorization': env.ACCELERATOR_APP_TOKEN
    }

    dcli = get_dcli()

    api_groups = {
        "pods": "v1",
        "persistentvolumeclaims": "v1",
    }
    
    # Get PVCs and Pods resources
    pvc_api = dcli.resources.get(api_version=api_groups["persistentvolumeclaims"], kind="PersistentVolumeClaim")
    pod_api = dcli.resources.get(api_version=api_groups["pods"], kind="Pod")

    try:
        pvcs = pvc_api.get(namespace=env.WKUBE_K8_NAMESPACE)
        pods = pod_api.get(namespace=env.WKUBE_K8_NAMESPACE)
    except ApiException as e:
        print(f"An error occurred: {e}")
        return []

    # Collect PVCs bound to running Pods
    bound_pvcs = set()
    bound_pvcs_pod_mapping = dict()
    for pod in pods.get('items', []):
        if pod['status']['phase'] == 'Running':
            for volume in pod.get('spec', {}).get('volumes', []):
                pvc_name = volume.get('persistentVolumeClaim', {}).get('claimName')
                if pvc_name:
                    bound_pvcs.add(pvc_name)
                    bound_pvcs_pod_mapping[pvc_name] = pod['metadata']['name']

    # Collect PVCs that are not bound to any running Pods
    unbound_or_non_running_pvcs = []
    for pvc in pvcs.get('items', []):
        pvc_name = pvc['metadata']['name']
        if pvc_name not in bound_pvcs:
            unbound_or_non_running_pvcs.append(pvc_name)
        else:
            # If PVC is bound to a Pod but the Pod is not running
            unbound_or_non_running_pvcs.append(pvc_name)
    
    remaining_pvcs = unbound_or_non_running_pvcs

    while True:
        batch = remaining_pvcs[:500]

        if not batch:
            break

        remaining_pvcs = remaining_pvcs[500:]

        res = http_client.request(
            "POST",
            f'{env.ACCELERATOR_CLI_BASE_URL}/v1/projects/filter-deletable-pvcs/',
            json=batch,
            headers=HEADERS
        )

        if res.status != 200:
            raise HTTPError(f"HTTP error occurred while requesting deleteable pvcs: Status code {res.status}")
        
        deleteable_pvcs = res.json()

        # Delete unmounted PVCs
        for pvc_name in deleteable_pvcs:
            #Check if it has associated pod
            pod_tbdel = bound_pvcs_pod_mapping.get(bound_pvcs_pod_mapping)
            if pod_tbdel:
                pod_resource.delete(name=pod_tbdel, namespace=env.WKUBE_K8_NAMESPACE, body=V1DeleteOptions(propagation_policy='Foreground'))
                print(f"Pod '{pod_tbdel} with pvc {pvc_name} has been deleted.")


            print(f"Deleting PVC: {pvc_name}")
            pvc_resource.delete(name=pvc_name, namespace=env.WKUBE_K8_NAMESPACE)





    