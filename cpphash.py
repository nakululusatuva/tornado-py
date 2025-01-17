from hexbytes import HexBytes


class cpphash(object):

    def __init__(self):
        pass

    '''
    @return (preimage, digest)
    '''
    @staticmethod
    def poseidon(preimages: list[HexBytes]) -> tuple[HexBytes, HexBytes]:
        pass

    '''
    @return (preimage, digest)
    '''
    @staticmethod
    def pedersen(preimages: list[HexBytes]) -> tuple[HexBytes, HexBytes]:
        pass