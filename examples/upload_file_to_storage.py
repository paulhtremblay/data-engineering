from google.cloud import storage


def upload_blob(
        bucket_name, 
        source_file_name, 
        destination_blob_name, 
        verbose = False
        ):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    #generation_match_precondition = 0
    #will overwrite
    generation_match_precondition = None

    blob.upload_from_filename(
            source_file_name, 
            if_generation_match=generation_match_precondition)

    if verbose:
        print(
            f"File {source_file_name} uploaded to {destination_blob_name}."
        )

