import urllib3
from urllib3.exceptions import HTTPError
from acc_worker.k8_gateway_actions.commons import get_dcli
from acc_worker.configs.Environment import get_environment_variables
from kubernetes.client.exceptions import ApiException


env = get_environment_variables()


retries = urllib3.util.Retry(total=10, backoff_factor=1)

http_client = urllib3.poolmanager.PoolManager(
    cert_reqs="CERT_NONE", num_pools=20, retries=retries
)

HEADERS = {
    'Content-Type': 'application/json',
    'x-authorization': env.ACCELERATOR_APP_TOKEN
}

def update_stalled_jobs_status():
    res = http_client.request(
        "GET",
        f'{env.ACCELERATOR_CLI_BASE_URL}/v1/projects/periodic-tasks/update-stalled-jobs-status/',
        headers=HEADERS
    )

    if res.status != 200:

        raise HTTPError(f"HTTP error occurred while updating stalled jobs status: Status code {res.status}")
    
    return res.json()
    

def delete_pvc(pvc_name):
    dcli = get_dcli()
    pvc_resource = dcli.resources.get(api_version='v1', kind='PersistentVolumeClaim')

    try:
        # 1️⃣ Patch the PVC to remove finalizers (if any)
        patch_body = {
            "metadata": {
                "finalizers": None
            }
        }
        pvc_resource.patch(
            name=pvc_name,
            namespace=env.WKUBE_K8_NAMESPACE,
            body=patch_body
        )
        print(f"Patched finalizers for PVC '{pvc_name}' in namespace '{env.WKUBE_K8_NAMESPACE}'.")

        # 2️⃣ Delete the PVC
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

  
    bound_pvcs = set()
    bound_pvcs_pod_mapping = dict()
    for pod in pods.get('items', []):
      
        for volume in pod.get('spec', {}).get('volumes', []):
            pvc_name = volume.get('persistentVolumeClaim', {}).get('claimName')
            if pvc_name:
                bound_pvcs.add(pvc_name)
                bound_pvcs_pod_mapping[pvc_name] = pod['metadata']['name']

   
    unbound_or_non_running_pvcs = []
    for pvc in pvcs.get('items', []):
        pvc_name = pvc['metadata']['name']
        if pvc_name not in bound_pvcs:
            unbound_or_non_running_pvcs.append(pvc_name)
        
    
    remaining_pvcs = unbound_or_non_running_pvcs

    while remaining_pvcs:
        batch = remaining_pvcs[:500]

        remaining_pvcs = remaining_pvcs[500:]

        res = http_client.request(
            "POST",
            f'{env.ACCELERATOR_CLI_BASE_URL}/v1/projects/periodic-tasks/filter-pending-pvcs/',
            json=batch,
            headers=HEADERS
        )

        if res.status != 200:
            raise HTTPError(f"HTTP error occurred while requesting deleteable pvcs: Status code {res.status}")
        
        pending_pvcs = set(res.json())
        batch_set = set(batch)

        for pvc_name in batch_set:
            if pvc_name in pending_pvcs:
                continue
            #Check if it has associated pod
            pod_tbdel = bound_pvcs_pod_mapping.get(pvc_name)
            if pod_tbdel:
                delete_opts = {
                    "apiVersion": "v1",
                    "kind": "DeleteOptions",
                    "propagationPolicy": "Foreground",  # or "Background"
                    # optional if you want to force fast deletion:
                    # "gracePeriodSeconds": 0,
                }

                pod_api.delete(
                    name=pod_tbdel,
                    namespace=env.WKUBE_K8_NAMESPACE,
                    body=delete_opts,
                )
                print(f"Pod '{pod_tbdel} with pvc {pvc_name} has been deleted.")


            print(f"Deleting PVC: {pvc_name}")
            # pvc_api.delete(name=pvc_name, namespace=env.WKUBE_K8_NAMESPACE)
            delete_pvc(pvc_name)





    