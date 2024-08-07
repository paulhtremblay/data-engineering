from google.cloud import storage

def delete_blob(bucket_name, blob_name, verbose = False):
    """Deletes a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    generation_match_precondition = None
    blob.reload()  
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)

    if verbose:
        print(f"Blob {blob_name} deleted.")

def delete_blob_with_pattern(prefix, bucket_name,
        verbose = False):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    for blob in blobs:
        if verbose:
            print(f'deleting {blob.name}')
        blob.delete()
