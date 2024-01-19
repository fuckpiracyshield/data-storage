from piracyshield_data_storage.blob.driver import BlobStorageDriver

class BlobStorage:

    """
    Blob storage manager.
    """

    driver = None

    def __init__(self, driver: BlobStorageDriver):
        self.driver = driver

    def upload(self, blob_name: str, file_path: str):
        try:
            return self.driver.upload(
                blob_name = blob_name,
                file_path = file_path
            )

        except:
            raise BlobStorageUploadException()

class BlobStorageUploadException(Exception):

    """
    Cannot upload the file.
    """

    pass
