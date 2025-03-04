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

input_mappings = os.environ.get('input_mappings')
output_mappings_mappings = os.environ.get('output_mappings')


async def remote_copy(source, destination):
    
    files = Fs.enumerate_files_by_prefix(source)
    
    async with aiohttp.ClientSession() as session:
        for file in files:
            file_url = Fs.get_file_url(file)
            destination_file = os.path.join(destination, os.path.relpath(file, source))

            # Make sure the directory exists in the destination
            os.makedirs(os.path.dirname(destination_file), exist_ok=True)

            print(f'Downloading file: {file_url}')
            async with session.get(file_url) as response:
                with open(destination_file, 'wb') as f:
                    f.write(await response.read())
    

async def input_mapping_from_mounted_storage(source, destination):
    # Check if the source exists
    if os.path.exists(source):
        # Make sure the destination directory exists
        dest_dir = os.path.dirname(destination)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)  # Create the directory if it doesn't exist
        
        # Remove the destination if it already exists (in case it's a file or symlink)
        if os.path.exists(destination):
            os.remove(destination)
        
        # Create the symlink
        os.symlink(source, destination)
    else:
        ValueError("The source does not exists.")


async def output_mapping_from_mounted_storage(source, destination):
    # Check if the source is a file or a folder
    if source.endswith('/'):
        # It's a folder, create a symlink to the folder
        os.symlink(source, destination)
    else:
        # It's a file, symlink to the folder containing the file
        source_dir = os.path.dirname(source)
        os.symlink(source_dir, destination)
        

async def main():
    tasks = []
    
    all_input_mappings = input_mappings.split(';')

    all_output_mappings = input_mappings.split(';')
    
    for input_mapping in all_input_mappings:
        
        input_mapping_new = input_mapping.replace('acc://', '__acc__')

        splitted_input_mapping_new = input_mapping_new.split(':')
        
        if len(splitted_input_mapping_new) != 2:
            raise ValueError("Invalid mapping syntax")
        
        source, destination = splitted_input_mapping_new

        if not (source.startswith('__acc__') or source.startswith('/mnt/data')):
            raise ValueError("Invalid source in input mappings")
        
        if not destination:
            if source.startswith('__acc__'):
                destination = f"/{source.split('__acc__')[1]}"

        if not destination.startswith('/'):
            raise ValueError("Invalid destination path: always use absolute path.")
            

        if source.startswith('__acc__'):
            source = source.split('__acc__')[1]
            tasks.append(remote_copy(source, destination))
        
        elif source.startswith('/mnt/data'):
            tasks.append(input_mapping_from_mounted_storage(source, destination))

    
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
            
        if destination.startswith('/mnt/data'):
            tasks.append(output_mapping_from_mounted_storage(destination, source))


    # Await all tasks
    # await asyncio.gather(*tasks)
    for task in tasks:
        await task


# Run the async function
asyncio.run(main())
