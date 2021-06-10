import hashlib
import logging

def content_hash(f, block_size=4*(1024**2)):

    hasher = hashlib.sha256()
    buffer = f.read(block_size)

    while len(buffer) > 0:

        hasher.update(hashlib.sha256(buffer).digest())
        buffer = f.read(block_size)

    return hasher.hexdigest()


def check_hashes(localpath, remotepath, connection):

    with connection.open(remotepath, 'rb', 32768) as f:

        remotehash = content_hash(f)
        logging.debug(f'Remote content hash:\t{remotehash}')

    with open(localpath, 'rb') as f:

        localhash = content_hash(f)
        logging.debug(f'Local content hash:\t{localhash}')

    return remotehash == localhash