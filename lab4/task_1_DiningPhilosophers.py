import logging

from time import time, sleep
from multiprocessing import Process
logging.basicConfig()

from kazoo.client import KazooClient


WAIT_EAT_MS = 1.25
WAIT_AFTER_ALL_DONE = 0.4


class Philosopher(Process):
    def __init__(self, root: str, id: int, fork_path: str, eat_seconds: int = 15, max_id: int = 5):
        super().__init__()
        self.url = f'{root}/{id}'
        self.root = root
        self.fork = fork_path
        self.id = id
        self.left_fork_id = id
        # Assign zero, in order to make circle (table!)
        self.right_fork_id = id + 1 if id + 1 < max_id else 0
        self.eat_seconds = eat_seconds

        self.counter = 0
        # We will count each time P is thinking, and each time he eat - we will decrease counter by 1
        # Thanks to this, each P will have equal eat-time (or +-1 from max) 
        self.action_counter = 0

        self.partner: Philosopher = None

    def run(self):
        assert self.partner is not None
        zk = KazooClient()
        zk.start()

        table_lock = zk.Lock(f'{self.root}/table', self.id)
        left_fork = zk.Lock(f'{self.root}/{self.fork}/{self.left_fork_id}', self.id)
        right_fork = zk.Lock(f'{self.root}/{self.fork}/{self.right_fork_id}', self.id)

        start = time()
        while time() - start < self.eat_seconds:
            print(f'Philosopher {self.id}: Im thinking')
            self.action_counter += 1
            # Only one philosopher could control table and do some action
            with table_lock:
                # Could philosopher take fork from left AND right ?
                if len(left_fork.contenders()) == 0 and len(right_fork.contenders()) == 0 \
                        and self.partner.action_counter < self.action_counter:
                    # Take them and start to eat!
                    left_fork.acquire()
                    right_fork.acquire()
            # If we take fork from left and right - start to each
            if left_fork.is_acquired and right_fork.is_acquired:
                print(f'Philosopher {self.id}: Im eating')
                self.counter += 1
                self.action_counter -= 1
                # Simulate some hard work (chew process!)
                sleep(WAIT_EAT_MS)

                left_fork.release()
                right_fork.release() 
            # Set some delay, in order to check prints in the console
            sleep(WAIT_AFTER_ALL_DONE)
            
        print(f'Philosopher with id={self.id} eat counter={self.counter} times action counter={self.action_counter}!')
        zk.stop()
        zk.close()


if __name__ == "__main__":
    master_zk = KazooClient()
    master_zk.start()
    # If tasks already runs, delete them
    if master_zk.exists('/task1'):
        master_zk.delete('/task1', recursive=True)

    master_zk.create('/task1')
    master_zk.create('/task1/table')
    master_zk.create('/task1/forks')
    master_zk.create('/task1/forks/1')
    master_zk.create('/task1/forks/2')
    master_zk.create('/task1/forks/3')
    master_zk.create('/task1/forks/4')
    master_zk.create('/task1/forks/5')

    root = '/task1'
    fork_path = 'forks'
    seconds_eat = 100
    p_list = [
        Philosopher(root, 0, fork_path, seconds_eat)
    ]
    for i in range(1, 5):
        p = Philosopher(root, i, fork_path, seconds_eat)
        p.partner = p_list[-1]

        p_list.append(p)
    
    p_list[0].partner = p_list[-1]
    
    for p in p_list:    
        p.start()

