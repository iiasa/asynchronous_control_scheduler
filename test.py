# from k8_gateway_actions.dispatch_build_and_push import BuildOCIImage

# BuildOCIImage("https://github.com/iiasa/generic_scenario_explorer_backend.git", "main", dockerfile="Dockerfile")

import time
current_time_ns = time.perf_counter_ns()
current_time_ns1 = time.perf_counter_ns()
print(current_time_ns, current_time_ns1)