from abc import ABC, abstractmethod

class BlobStorageDriver(ABC):

    @abstractmethod
    def upload(self, file_path: str):
        pass
