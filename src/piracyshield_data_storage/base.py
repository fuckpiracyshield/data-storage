import time

class BaseStorage(DatabaseArangodbDocument):

    start_counter = 0

    stop_counter = 0

    def __init__(self):
        super().__init__()

    def _start_counter(self):
        self.start_counter = time.time()

    def _stop_counter(self):
        self.stop_counter = time.time()
