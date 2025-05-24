import json
import base64
import urllib3
import traceback
from concurrent.futures import ThreadPoolExecutor
from kubernetes import client, config, watch
from acc_worker.configs.Environment import get_environment_variables

env = get_environment_variables()

config.load_kube_config_from_dict(
    config_dict=json.loads(
        base64.b64decode(env.WKUBE_SECRET_JSON_B64.encode())
    )
)

kube_config = client.Configuration().get_default_copy()

kube_config.verify_ssl = False

client.Configuration.set_default(kube_config)

# Create Kubernetes API client
v1 = client.CoreV1Api()

# Define API endpoint for sending events
API_ENDPOINT = f'{env.ACCELERATOR_CLI_BASE_URL}/v1/projects/webhook-event/'

if not env.ACCELERATOR_APP_TOKEN:
    raise ValueError("env.ACCELERATOR_APP_TOKEN is not set.")

HEADERS = {
    'Content-Type': 'application/json',
    'x-authorization': env.ACCELERATOR_APP_TOKEN
}

executor = ThreadPoolExecutor(max_workers=10)

# Configure HTTP client with retries
retries = urllib3.util.Retry(total=10, backoff_factor=1)
http_client = urllib3.PoolManager(cert_reqs="CERT_NONE", num_pools=20, retries=retries)


def send_event(event):
    """Send a single event to the API with error handling."""
    try:
        print(f"Sending event: {event}")

        res = http_client.request(
            "POST",
            API_ENDPOINT,
            body=json.dumps({'type': 'WKUBE_POD_EVENT', 'data': event}),
            headers=HEADERS
        )

        if res.status >= 400:
            try:
                error_details = json.loads(res.data.decode("utf-8"))
            except json.JSONDecodeError:
                error_details = res.data.decode("utf-8")

            print(f"\n❌ HTTP error {res.status} while sending event: {error_details}\n")
        else:
            print(f"✅ Event sent successfully (status code: {res.status})")

    except Exception:
        print(f"\n❌ Error while sending event:\n{traceback.format_exc()}\n")


def extract_event_data(event):
    """Extract relevant event data from Kubernetes event."""
    event_object = event['object']

    values = {
        "timestamp": event_object.metadata.creation_timestamp.isoformat(),
        "uid": event_object.metadata.uid,
        "reason": getattr(event_object, 'reason', "N/A"),
        "message": getattr(event_object, 'message', "N/A")
    }

    if hasattr(event_object, 'involved_object'):
        involved_object = event_object.involved_object
        values['kind'] = involved_object.kind
        values['involved_object_name'] = involved_object.name

        if values['kind'] == 'Pod':
            values['task_id'] = involved_object.name.rsplit('-', 1)[0]
        elif values['kind'] == 'Job':
            values['task_id'] = involved_object.name
        else:
            print(f"Ignoring event from kind {values['kind']}")

    return values


def process_event(event):
    """Extract and send the event asynchronously."""
    executor.submit(send_event, extract_event_data(event))


def watch_and_process_events():
    """Watch Kubernetes events and process them in parallel."""
    try:
        while True:
            try:
                w = watch.Watch()
                for event in w.stream(v1.list_namespaced_event, env.WKUBE_K8_NAMESPACE):
                    process_event(event)

            except client.exceptions.ApiException as e:
                if e.status == 410:
                    print("Resource version expired. Reconnecting...")
                    continue
                else:
                    print(f"API exception: {e}")
                    break
            except Exception as e:
                print(f"Unexpected error: {e}")
                break
    finally:
        print("Shutting down event watcher.")
        executor.shutdown(wait=True)  # Ensure threads are closed


if __name__ == '__main__':
    watch_and_process_events()
