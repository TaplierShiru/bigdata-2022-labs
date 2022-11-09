import logging
logging.basicConfig()

from kazoo.client import KazooClient


class Animal: 
    def __init__(self, root: str, name: str, party_size: int):
        self.root = root
        self.name = name
        self.url = f'{root}/{name}'
        self.party_size = party_size

        self.zk = KazooClient()
        self.zk.start()

        @self.zk.ChildrenWatch('/zoo')
        def watch_node(children):
            if len(children) < self.party_size:
                print('Waiting for the others.')
            elif len(children) == self.party_size:
                print('Zoo is full')
            else:
                print('Zoo is crowded')

    def enter(self):
        self.zk.create(self.url, ephemeral=True)

    def leave(self):
        self.zk.delete(self.url)
        self.zk.stop()


if __name__ == "__main__":
    zk = KazooClient()
    zk.start()
    if not zk.exists('/zoo'):
        zk.create('/zoo')

    animal_1 = Animal('/zoo', 'cat_1', 2)
    animal_1.enter()

    animal_2 = Animal('/zoo', 'cat_2', 2)
    animal_2.enter()

    print(zk.get_children('/zoo'))

    animal_2.leave()
    animal_1.leave()

    zk.stop()
    zk.close()

