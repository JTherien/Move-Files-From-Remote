import hashlib

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

    with open(localpath, 'rb') as f:

        localhash = content_hash(f)

    return remotehash == localhash