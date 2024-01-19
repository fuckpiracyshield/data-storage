from piracyshield_data_storage.account.storage import AccountStorage

class GuestStorage(AccountStorage):

    COLLECTION = 'guests'

    collection_instance = None

    def __init__(self):
        super().__init__(collection_name = self.COLLECTION)
