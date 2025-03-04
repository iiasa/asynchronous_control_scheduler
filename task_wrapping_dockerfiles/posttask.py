import os
import ssl
import asyncio
import aiohttp
import aiofiles
import shutil
from pathlib import Path
from accli import Fs, AjobCliService

ssl._create_default_https_context = ssl._create_unverified_context

file_url = Fs.get_file_url(input_file)

output_mappings = os.environ.get('output_mappings')

all_mappings = output_mappings.split(';')


async def get_files(source):
    """Retrieve a list of files from a directory or single file recursively."""
    if os.path.isfile(source):
        return [source]
    elif os.path.isdir(source):
        file_list = []
        for root, _, files in os.walk(source):
            for file in files:
                file_list.append(os.path.join(root, file))
        return file_list
    else:
        raise FileNotFoundError(f"Source '{source}' does not exist.")

async def remote_push(source, destination):
    """Uploads files from source to the remote destination one by one."""
    try:
        files = await get_files(source)
        
        for file in files:
            print(f"Uploading {file} to {destination}...")
            Fs.write_file(file, file)
            print(f"Uploaded {file} successfully.")
        
    except Exception as e:
        print(f"Error: {e}")
    

async def main():
    tasks = []
    
    for output_mapping in output_mappings:
        output_mapping_new = output_mapping.replace('acc://', '__acc__')

        splitted_output_mapping_new = output_mapping_new.split(':')

        if len(splitted_output_mapping_new) != 2:
            raise ValueError("Invalid mapping syntax")

        source, destination = splitted_output_mapping_new

        if (source.startswith('__acc__')):
            raise ValueError("Invalid source in output mappings")

        if not (source.startswith('/')):
            raise ValueError("Please use absolute uri")

        if not (destination.startswith('__acc__') or destination.startswith('/mnt/data')):
            raise ValueError("Invalid destination in output mappings")

        
        if not destination:
            destination = f"__acc__{source}"
            
            
        if destination.startswith('__acc__'):
            destination = destination.split('__acc__')[1]
            tasks.append(remote_push(source, destination))
        

    # Await all tasks
    # await asyncio.gather(*tasks)
    for task in tasks:
        await task


# Run the async function
asyncio.run(main())
