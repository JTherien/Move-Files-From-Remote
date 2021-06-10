import os
import yaml
import pysftp
from libs.hash import check_hashes

with open('config.yaml', 'r') as stream:

    config = yaml.safe_load(stream)

cnopts = pysftp.CnOpts()

# Need to change this
cnopts.hostkeys = None

connection_attempts = 1

while True:

    try:

        sftp = pysftp.Connection(
            config['address'],
            username=config['user'],
            password=config['password'],
            cnopts=cnopts)

        print('Successfully established SFTP connection.')

        break

    except Exception as e:

        if connection_attempts <= 10:

            print(f'Retrying connection {connection_attempts}/10.')
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

            sftp.execute(f'rm {remotepath}')
            break

        else:

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
            
            print(f'Downloading {entry}')
            move_from_remote(localpath, remotepath)

        else:
            
            local_file_exists_message = f'{entry} exists in local directory.'

            if check_hashes(localpath, remotepath, sftp):

                local_file_exists_message += 'Content hashes match.'
                print(local_file_exists_message)
                sftp.execute(f'rm {remotepath}')

            else:

                local_file_exists_message += ' Content hashes do not match. Retrying download.'
                print(local_file_exists_message)
                move_from_remote(localpath, remotepath)

sftp.close()