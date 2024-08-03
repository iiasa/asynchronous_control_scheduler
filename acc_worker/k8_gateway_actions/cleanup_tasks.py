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
            namespace=env.WKUBE_K8_NAMESPACE
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

    # Get all PVCs in the specified namespace
    pvc_resource = dcli.resources.get(api_version='v1', kind='PersistentVolumeClaim')
    all_pvcs = pvc_resource.get(namespace=env.WKUBE_K8_NAMESPACE)

    # Get all Pods in the specified namespace
    pod_resource = dcli.resources.get(api_version='v1', kind='Pod')
    all_pods = pod_resource.get(namespace=env.WKUBE_AUTO_GITHUB_PAT)

    # Extract PVC names from Pods
    mounted_pvc_names = set()
    for pod in all_pods.get('items', []):
        volumes = pod.get('spec', {}).get('volumes', [])
        for volume in volumes:
            if 'persistentVolumeClaim' in volume:
                pvc_name = volume['persistentVolumeClaim']['claimName']
                mounted_pvc_names.add(pvc_name)

    # Identify unmounted PVCs
    unmounted_pvcs = []
    for pvc in all_pvcs.get('items', []):
        pvc_name = pvc['metadata']['name']
        if pvc_name not in mounted_pvc_names:
            unmounted_pvcs.append(pvc_name)

    
    remaining_pvcs = unmounted_pvcs

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
            print(f"Deleting PVC: {pvc_name}")
            pvc_resource.delete(name=pvc_name, namespace=env.WKUBE_K8_NAMESPACE)





    