from contextlib import redirect_stdout, redirect_stderr
from tests.child import child
import traceback

def main():
    with open('testlogfile.log', 'w+') as log_stream:

        with redirect_stdout(log_stream):
            try:
                child()
                
            except Exception as err:
                error_message = ''.join(traceback.format_exc())
                log_stream.write(error_message)