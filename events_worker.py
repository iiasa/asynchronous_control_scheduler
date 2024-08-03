import time
import json
import base64
import urllib3
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from kubernetes import client, config, watch
from acc_worker.configs.Environment import get_environment_variables


env = get_environment_variables()

config.load_kube_config_from_dict(
    config_dict=json.loads(
        base64.b64decode(
            env.WKUBE_SECRET_JSON_B64.encode()
        )
    )
)

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


retries = urllib3.util.Retry(total=10, backoff_factor=1)

http_client = urllib3.poolmanager.PoolManager(
    cert_reqs="CERT_NONE", num_pools=20, retries=retries
)

# Function to send an event to the external API
def send_event(event):

    try:

        print("Event:")
        print(event)
        
        res = http_client.request(
            "POST",
            API_ENDPOINT,
            json={
                'type': 'WKUBE_POD_EVENT',
                'data': event
            },
            headers=HEADERS
        )

        
        if str(res.status)[0] in ['4', '5'] and res.json():
            print(f"\nHttp error while sending events to control server.{res.json()}\n") 
        
    except Exception as err:
        error_message = ''.join(traceback.format_exc())
        print(f"\nError while sending events to control server. \n {error_message} \n")
        

def extract_event_data(event):

    event_object = event['object']

    values = dict(
        timestamp=event_object.metadata.creation_timestamp.isoformat(),
        uid=event_object.metadata.uid
    )

    for attr in ['reason', 'message']:
        if not hasattr(event_object, attr):
            print(f"Event object has no attribute {attr}")

        else:
            values[attr] = getattr(event_object, attr)

    
    if hasattr(event_object, 'involved_object'):
        involved_object = getattr(event_object, 'involved_object')
        values['kind'] = involved_object.kind

        values['involved_object_name'] = involved_object.name

        if values['kind'] == 'Pod':
            values['task_id'] = involved_object.name.rsplit('-', 1)[0]
        elif values['kind'] == 'Job':
            values['task_id'] = involved_object.name
        else:
            print(f"Ignoring the event from kind {values['kind']}")

    return values

    

# Function to process a batch of events
def process_events(events):
    if events:
        futures = [executor.submit(send_event, extract_event_data(event)) for event in events]
    

# Function to start a timer to process events if batch size is not reached
def start_batch_timer(events, batch_lock, batch_event, interval=30):
    
    def timer_callback():
        try:
            with batch_lock:
                if batch_event['events']:
                    process_events(batch_event['events'])
                    batch_event['events'].clear()

                print("Batch processed and cleared via callback \n")
                
                start_batch_timer(events, batch_lock, batch_event, interval)
        except Exception as err:
            print("\nError while processing callback exception.")
            error_message = ''.join(traceback.format_exc())
            print(f"{error_message} \n\n")

    
    timer = threading.Timer(interval, timer_callback)
    timer.start()
    
    return timer

# Function to watch and process events
def watch_and_process_events():
    global timer  # Declare the timer variable as global
    timer = None

    try:
        while True:

            try:

                w = watch.Watch()
                event_batch = []
                batch_lock = threading.Lock()
                batch_event = {'events': event_batch}
                timer = start_batch_timer(event_batch, batch_lock, batch_event, interval=30)

                for event in w.stream(v1.list_namespaced_event, env.WKUBE_K8_NAMESPACE):
                    with batch_lock:
                        event_batch.append(event)
                        # If batch size reaches threshold, process immediately
                        if len(event_batch) >= 10:  # Adjust batch size as needed
                            process_events(event_batch)
                            event_batch.clear()
                            print("Batch processed and cleared after threshold is reached. \n")
                            # Restart the timer
                            timer.cancel()
                            timer = start_batch_timer(event_batch, batch_lock, batch_event, interval=30)

                # Process any remaining events after exiting the loop
                with batch_lock:
                    if event_batch:
                        process_events(event_batch)

            except client.exceptions.ApiException as e:
                if e.status == 410:
                    print("Resource version expired. Reconnecting...")
                    continue  # Restart the watch
                else:
                    print(f"API exception occurred: {e}")
                    break
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                break
    finally:
        # Ensure the timer is cancelled if it's still running
        if timer:
            timer.cancel()

        

if __name__ == '__main__':
    watch_and_process_events()
