from k8_gateway_actions.dispatch_build_and_push import BuildOCIImage

BuildOCIImage("https://github.com/iiasa/generic_scenario_explorer_backend.git", "main", dockerfile="Dockerfile")