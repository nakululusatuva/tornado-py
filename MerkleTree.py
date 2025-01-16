from hexbytes import HexBytes

import Log


class hashzoo(object):

    @staticmethod
    def poseidon(preimages: list[HexBytes]) -> HexBytes:
        pass

    @staticmethod
    def pedersen(preimages: list[HexBytes]) -> HexBytes:
        pass


class MerkleTree(object):

    # 21888242871839275222246405745257275088548364400416034343698204186575808495617
    FILED_SIZE: HexBytes = HexBytes.fromhex('30644E72E131A029B85045B68181585D2833E84879B9709143E1F593F0000001')

    # 21663839004416932945382355908790599225266501822907911457504978515578255421292
    # Keccak256("tornado") % FILED_SIZE
    ZERO_VALUE: HexBytes = HexBytes.fromhex('2FE54C60D3ACABF3343A35B6EBA15DB4821B340F76E741E2249685ED4899AF6C')

    def __init__(self, height: int, leafs: list[HexBytes] = None) -> None:
        self.TAG        : str                  = __class__.__name__
        self.leafs      : list[HexBytes]       = leafs if leafs is not None else []
        self.layers     : list[list[HexBytes]] = [[] for _ in range(height + 1)]  # The last layer is the root
        self.capacity   : int = 2 ** height
        if len(self.leafs) > self.capacity:
            raise Exception(f'Leafs count exceeds capacity: {len(self.leafs)} > {self.capacity}')
        if len(self.leafs) == 0:
            self.add(MerkleTree.ZERO_VALUE)
        else:
            for leaf in self.leafs:
                self.add(leaf)

    def root(self) -> HexBytes:
        return self.layers[-1][0]

    '''
    Get leaf value by index
    @return None if index is out of range
    '''
    def leaf(self, index: int) -> HexBytes | None:
        return self.leafs[index] if 0 <= index < len(self.leafs) else None

    '''
    Get path to root from leaf index
    @return [(Leaf_L, Leaf_R), (Parent_L, Parent_R), ..., (Root)]
            None if index is out of range
    '''
    def path(self, leaf_index: int) -> list[tuple[HexBytes, HexBytes]] | None:
        path      : list[tuple[HexBytes, HexBytes]] = []
        node_index: int                             = leaf_index
        for i in range(0, len(self.layers)):
            if i == len(self.layers) - 1:
                path.append((self.layers[i][0], MerkleTree.ZERO_VALUE))
            if self.is_left(node_index):
                node_left : HexBytes = self.layers[i][node_index]
                node_right: HexBytes = MerkleTree.ZERO_VALUE
            else:
                node_left : HexBytes = self.leaf(node_index - 1)
                node_right: HexBytes = self.layers[i][node_index]
            node_index //= 2
            path.append((node_left, node_right))
        return path

    '''
    Add a leaf to the tree
    @return True on succeed
            False if leaf value is out of range
    '''
    def add(self, leaf: HexBytes) -> bool:
        # Check if legal
        if int.from_bytes(leaf) >= int.from_bytes(MerkleTree.FILED_SIZE):
            Log.Error(self.TAG, f'Leaf value out of range: {leaf.to_0x_hex()}')
            return False
        elif len(self.leafs) >= self.capacity:
            Log.Error(self.TAG, f'Tree is full')
            return False

        # Prepare left leaf, right leaf and their parent
        self.leafs.append(leaf)
        node_index: int  = len(self.leafs) - 1
        if self.is_left(node_index):
            add_parent: bool     = True
            node_left : HexBytes = leaf
            node_right: HexBytes = MerkleTree.ZERO_VALUE
        else:
            add_parent: bool     = False
            node_left : HexBytes = self.leaf(node_index - 1)
            node_right: HexBytes = leaf
        parent: HexBytes = hashzoo.poseidon([node_left, node_right])

        # Update merkle tree
        for i in range(0, len(self.layers)):
            node_index //= 2
            if add_parent:
                self.layers[i].append(parent)
            else:
                self.layers[i][node_index] = parent
            if self.is_left(node_index):
                add_parent = True
                node_left  = parent
                node_right = MerkleTree.ZERO_VALUE
            else:
                add_parent = False
                node_left  = self.layers[i][node_index - 1]
                node_right = parent
            parent = hashzoo.poseidon([node_left, node_right])

        return True

    @staticmethod
    def is_left(node_index: int) -> bool:
        return node_index % 2 == 0

    @staticmethod
    def is_right(node_index: int) -> bool:
        return node_index % 2 == 1
