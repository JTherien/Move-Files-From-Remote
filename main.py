import os
import yaml
import pysftp
import logging
from libs.hash import check_hashes

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s', 
    datefmt='%d-%b-%y %H:%M:%S')

with open('config.yaml', 'r') as stream:

    config = yaml.safe_load(stream)

cnopts = pysftp.CnOpts()

# Need to change this
cnopts.hostkeys = None

connection_attempts = 1

logging.info(f'Attempting to establish SFTP connection with {config["address"]}')

while True:

    try:

        sftp = pysftp.Connection(
            config['address'],
            username=config['user'],
            password=config['password'],
            cnopts=cnopts)

        logging.info(f'Successfully connected to {config["address"]}')

        break

    except Exception as e:

        if connection_attempts <= 10:

            logging.warning(f'Unable to connect to {config["address"]}. Retrying {connection_attempts}/10')
            connection_attempts += 1
        
        else:

            print(e.message)
            exit

def move_from_remote(localpath, remotepath, sftp=sftp, attempts=10):
    
    retry = 1

    while retry <= attempts:

        sftp.get(remotepath, localpath, preserve_mtime=True)

        # Validate content hashes prior to deleting remote file     
        if check_hashes(localpath, remotepath, sftp):

            logging.info(f'Successfully moved {entry}. Deleting remote copy')
            sftp.execute(f'rm {remotepath}')
            break

        else:

            logging.warning(f'Unsuccessfully moved {entry}. Hash values do not match. Retrying {retry}/{attempts}')
            print(f'{entry} did not import correctly. Retrying {retry}/{attempts}')
            os.remove(localpath)
            retry+=1

for entry in sftp.listdir(config['path-motion']):

    entry_extension = entry.split('.')[1]

    if entry_extension in ['png', 'jpg', 'gif']:

        remotepath = f'{config["path-motion"]}{entry}'
        localpath = os.path.join(config['path-local'], entry)
        mode = sftp.stat(remotepath).st_mode

        if not os.path.isfile(localpath):
            
            logging.info(f'Downloading {entry}')
            move_from_remote(localpath, remotepath)

        else:
            
            local_file_exists_message = f'{entry} exists in local directory'

            if check_hashes(localpath, remotepath, sftp):

                local_file_exists_message += ' Content hashes match'
                logging.info(local_file_exists_message)
                sftp.execute(f'rm {remotepath}')

            else:

                local_file_exists_message += f' Content hashes do not match. Downloading {entry}'
                logging.warning(local_file_exists_message)
                move_from_remote(localpath, remotepath)

sftp.close()