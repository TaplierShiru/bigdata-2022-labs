import logging
import threading
import random

from multiprocessing import Process
from time import sleep
from typing import Optional

logging.basicConfig()

from kazoo.client import KazooClient


ACTION_COMMIT =   b'commit'
ACTION_ROLLBACK = b'rollback'

WAIT_HARD_WORK_SECONDS = 5


class Client(Process):
    def __init__(self, root: str, id: int):
        super().__init__()
        self.url = f'{root}/{id}'
        self.root = root
        self.id = id

    def run(self):
        zk = KazooClient()
        zk.start()

        value = ACTION_COMMIT if random.random() > 0.5 else ACTION_ROLLBACK
        print(f'Client {self.id} request {value.decode()}')
        zk.create(self.url, value, ephemeral=True)
        
        @zk.DataWatch(self.url)
        def watch_myself(data, stat):
            if stat.version != 0:
                print(f'Client {self.id} do {data.decode()}')

        sleep(WAIT_HARD_WORK_SECONDS)

        zk.stop()
        zk.close()


class Coordinator:

    timer: Optional[threading.Timer] = None

    @staticmethod
    def main(number_of_clients = 3, duration = 2):
        coordinator = KazooClient()
        coordinator.start()

        if coordinator.exists('/task_2'):
            coordinator.delete('/task_2', recursive=True)

        coordinator.create('/task_2')
        coordinator.create('/task_2/transaction')

        Coordinator.timer = None

        def check_clients():
            tr_clients = coordinator.get_children('/task_2/transaction')
            commit_counter = 0
            abort_counter = 0
            for client in tr_clients:
                # First element - action value
                commit_counter += int(coordinator.get(f'/task_2/transaction/{client}')[0] == ACTION_COMMIT)
                abort_counter +=  int(coordinator.get(f'/task_2/transaction/{client}')[0] == ACTION_ROLLBACK)

            # All clients should commit, otherwise - rollback
            final_action = ACTION_COMMIT if commit_counter == number_of_clients else ACTION_ROLLBACK
            for client in tr_clients:
                coordinator.set(f'/task_2/transaction/{client}', final_action)

        @coordinator.ChildrenWatch('/task_2/transaction')
        def watch_clients(clients):
            if len(clients) == 0:
                if Coordinator.timer is not None:
                    Coordinator.timer.cancel()
            else:
                if Coordinator.timer is not None:
                    Coordinator.timer.cancel()
                # After some time - check status of the clients
                Coordinator.timer = threading.Timer(duration, check_clients)
                Coordinator.timer.daemon = True
                Coordinator.timer.start()

            if len(clients) < number_of_clients:
                print(f'Waiting for the others. clients={clients}')
            elif len(clients) == number_of_clients:
                print(f'Check clients')
                Coordinator.timer.cancel()
                check_clients()

        root = '/task_2/transaction'
        for i in range(number_of_clients):
            p = Client(root, i)
            p.start()


if __name__ == "__main__":
    Coordinator.main()
