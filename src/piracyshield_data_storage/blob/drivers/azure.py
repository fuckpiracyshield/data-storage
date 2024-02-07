from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobType

from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError, AzureError

from piracyshield_data_storage.blob.driver import BlobStorageDriver

class AzureBlobStorage(BlobStorageDriver):

    """
    Azure blob storage manage.
    """

    def __init__(self, connection_string: str, container_name: str):
        """
        Initializes the client expecting a connection string.
        The connection string exploded looks something like:
            DefaultEndpointsProtocol=http;
            AccountName=ACCOUNT_NAME;
            AccountKey=ACCOUNT_KEY;
            BlobEndpoint=http://ACCOUNT_NAME.blob.localhost:10000;
            QueueEndpoint=http://ACCOUNT_NAME.queue.localhost:10001;
            TableEndpoint=http://ACCOUNT_NAME.table.localhost:10002;

        :param connection_string: Azure connection string (https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string).
        :param container_name: container name in use.
        """
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        self.container_client = self.blob_service_client.get_container_client(container_name)

    def create_container(self):
        """
        Creates the container.
        Currently used to create a mock blob storage.
        """

        return self.container_client.create_container()

    def upload(self, blob_name: str, file_path: str) -> bool | Exception:
        """
        Uploads a new file in chunks to handle big files safely.

        :param blob_name: name of the file to store.
        :param data: file content.
        :return bool if correct.
        """

        try:
            blob_client = self.container_client.get_blob_client(blob_name)

            blob_client.upload_blob("", blob_type = BlobType.BlockBlob)

            # upload in small chunks
            with open(file_path, "rb") as file:
                block_size = 4 * 1024 * 1024    # 4 MB chunks

                block_ids = []

                block_id_prefix = "block"

                block_number = 0

                while True:
                    data = file.read(block_size)

                    if not data:
                        break

                    block_id = block_id_prefix + str(block_number).zfill(6)

                    blob_client.stage_block(block_id, data)

                    block_ids.append(block_id)

                    block_number += 1

                # commit the blocks and create the blob
                blob_client.commit_block_list(block_ids)

            return True

        except ResourceExistsError:
            raise AzureBlobStorageAlreadyExistsException()

        except (ResourceNotFoundError, AzureError):
            raise AzureBlobStorageUploadException()

    def get_list(self):
        """
        Returns a list of blobs in the container.

        :return a paginated list of the files.
        """

        return self.container_client.list_blobs()

    def remove(self, blob_name: str) -> bool | Exception:
        """
        Removes the blob from the container.

        :param blob_name: the file name to be removed.
        :return bool if correct.
        """

        try:
            blob_client = self.container_client.get_blob_client(blob_name)

            blob_client.delete_blob()

        except (ResourceNotFoundError, AzureError):
            raise AzureBlobStorageRemoveException()

class AzureBlobStorageAlreadyExistsException(Exception):

    """
    The blob already exists.
    """

    pass

class AzureBlobStorageUploadException(Exception):

    """
    The blob cannot be uploaded.
    """

    pass

class AzureBlobStorageRemoveException(Exception):

    """
    The blob cannot be removed.
    """

    pass
