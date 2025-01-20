import threading as TR
from cpphash import cpphash
from enum import Enum
from hexbytes import HexBytes

import Log


class ImplType(Enum):
    MEMORY = 'memory'


class Interface(object):

    # 21888242871839275222246405745257275088548364400416034343698204186575808495617
    FILED_SIZE: HexBytes = HexBytes.fromhex('30644E72E131A029B85045B68181585D2833E84879B9709143E1F593F0000001')

    # 21663839004416932945382355908790599225266501822907911457504978515578255421292
    # Keccak256("tornado") % FILED_SIZE
    ZERO_VALUE: HexBytes = HexBytes.fromhex('2FE54C60D3ACABF3343A35B6EBA15DB4821B340F76E741E2249685ED4899AF6C')

    def __init__(self, _type: ImplType) -> None:
        self._type: ImplType = _type

    @staticmethod
    def is_left(node_index: int) -> bool:
        return node_index % 2 == 0

    def implementation(self) -> ImplType:
        return self._type

    def size(self):
        raise NotImplementedError

    '''
    Get root value
    @return HexBytes of root value
            None if tree is empty
    '''
    def root(self) -> HexBytes | None:
        raise NotImplementedError

    '''
    Get leaf value by index
    @return None if index is out of range
    '''
    def leaf(self, index: int) -> HexBytes | None:
        raise NotImplementedError

    '''
    Get path to root from leaf
    @return [(Leaf_L, Leaf_R), (Parent_L, Parent_R), ..., (Root, None)]
            None if HexBytes is exists
    '''
    def path(self, leaf: HexBytes) -> list[tuple[HexBytes, HexBytes | None]] | None:
        raise NotImplementedError

    '''
    Add a leaf to the tree
    @return True on succeed
            False if leaf value is out of range
    '''
    def add(self, leaf: HexBytes) -> bool:
        raise NotImplementedError


class Memory(Interface):

    def __init__(self, height: int) -> None:
        super().__init__(ImplType.MEMORY)
        self.TAG     : str                  = __class__.__name__
        self.mutex   : TR.RLock             = TR.RLock()
        self.height  : int                  = height
        self.layers  : list[list[HexBytes]] = [[] for _ in range(height + 1)]  # [[leafs], [parents], [root]]
        self.capacity: int                  = 2 ** height
        self._size   : int                  = 0

    def size(self):
        with self.mutex:
            return self._size

    def root(self) -> HexBytes | None:
        with self.mutex:
            if 0 == self._size:
                return None
            return self.layers[-1][0]

    def leaf(self, index: int) -> HexBytes | None:
        with self.mutex:
            return self.layers[0][index] if 0 <= index < len(self.layers[0]) else None

    def path(self, leaf: HexBytes) -> list[tuple[HexBytes, HexBytes | None]] | None:
        with self.mutex:
            # Check if tree empty
            if 0 == len(self.layers[0]):
                Log.Error(self.TAG, f'Tree is empty')
                return None

            # Get leaf index of commitment
            node_index: int = -1
            for i in range(0, len(self.layers[0])):
                if self.layers[0][i] == leaf:
                    node_index = i
                    break
            if node_index == -1:
                Log.Error(self.TAG, f'Leaf not found: {leaf.to_0x_hex()}')
                return None

            # Build path
            path: list[tuple[HexBytes, HexBytes | None]] = []
            for level in range(0, self.height):
                if Interface.is_left(node_index):
                    node_left : HexBytes = self.layers[level][node_index]
                    node_right: HexBytes = self.layers[level][node_index + 1] if node_index + 1 < len(self.layers[level]) else Interface.ZERO_VALUE
                else:
                    node_left : HexBytes = self.layers[level][node_index - 1]
                    node_right: HexBytes = self.layers[level][node_index]
                path.append((node_left, node_right))
                node_index //= 2
            path.append((self.layers[self.height][0], None))

            return path

    def add(self, leaf: HexBytes) -> bool:
        with self.mutex:
            # Check if legal
            if int.from_bytes(leaf, byteorder='big') >= int.from_bytes(Interface.FILED_SIZE, byteorder='big'):
                Log.Error(self.TAG, f'Leaf value out of range: {leaf.to_0x_hex()}')
                return False
            elif len(self.layers[0]) >= self.capacity:
                Log.Error(self.TAG, f'Tree is full')
                return False

            # Prepare left leaf, right leaf and their parent
            self.layers[0].append(leaf)
            node_index: int = len(self.layers[0]) - 1
            if Interface.is_left(node_index):
                add_parent: bool     = True
                node_left : HexBytes = leaf
                node_right: HexBytes = Interface.ZERO_VALUE
            else:
                add_parent: bool     = False
                node_left : HexBytes = self.leaf(node_index - 1)
                node_right: HexBytes = leaf
            parent: HexBytes = cpphash.poseidon([node_left, node_right])[1]

            # Re-build merkle tree
            for i in range(1, self.height):
                node_index += 1 if 0 == node_index % 2 else 0
                node_index //= 2
                if add_parent:
                    self.layers[i].append(parent)
                else:
                    self.layers[i][node_index] = parent
                if Interface.is_left(node_index):
                    add_parent = True
                    node_left  = self.layers[i][node_index]
                    node_right = Interface.ZERO_VALUE
                else:
                    add_parent = False
                    node_left  = self.layers[i][node_index - 1]
                    node_right = self.layers[i][node_index]
                parent = cpphash.poseidon([node_left, node_right])[1]
            self.layers[self.height][0] = parent

            self._size += 1
            return True


def Create(impl: ImplType, height: int) -> Interface:
    if impl == ImplType.MEMORY:
        return Memory(height)
    else:
        raise NotImplementedError
