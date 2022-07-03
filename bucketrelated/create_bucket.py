from google.cloud import storage

if __name__ == '__main__':
    storage_client = storage.Client()
    bucket = storage_client.bucket("bwt-createbucket-session")
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location='US')

    print("bucket name: " + str(new_bucket.name))
    print("bucket storage class: " + str(new_bucket.storage_class))
    print("bucket location: " + str(new_bucket.location))