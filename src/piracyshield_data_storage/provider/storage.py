from piracyshield_data_storage.account.storage import AccountStorage

class ProviderStorage(AccountStorage):

    COLLECTION = 'providers'

    collection_instance = None

    def __init__(self):
        super().__init__(collection_name = self.COLLECTION)
