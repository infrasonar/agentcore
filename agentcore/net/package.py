from __future__ import annotations
import logging
import msgpack
import struct
from typing import Optional, Any


class Package(object):

    __slots__ = ('partid', 'length', 'pid', 'tp', 'body', 'data', 'total')

    st_package = struct.Struct('<QIHBB')

    def __init__(self, barray: Optional[bytearray] = None):
        if barray is None:
            return

        self.partid, self.length, self.pid, self.tp, checkbit = \
            self.__class__.st_package.unpack_from(barray, offset=0)
        if self.tp != checkbit ^ 255:
            raise ValueError('invalid checkbit')
        self.total = self.__class__.st_package.size + self.length
        self.body: Optional[bytearray] = None
        self.data: Any = None

    @classmethod
    def make(
        cls,
        tp: int,
        data: Any = b'',
        pid: int = 0,
        partid: int = 0,
        is_binary: bool = False,
    ) -> Package:
        pkg = cls()
        pkg.tp = tp
        pkg.pid = pid
        pkg.partid = partid

        if is_binary is False:
            data = msgpack.packb(data)

        pkg.body = data
        pkg.length = len(data)
        return pkg

    def to_bytes(self) -> bytes:
        header = self.st_package.pack(
            self.partid,
            self.length,
            self.pid,
            self.tp,
            self.tp ^ 0xff)

        assert self.body is not None
        return header + self.body

    @classmethod
    def from_bytes(cls, barray: bytearray) -> Package:
        pkg = cls(barray)
        pkg.body = barray[cls.st_package.size:pkg.total]
        return pkg

    def extract_data_from(self, barray: bytearray):
        self.body = None
        try:
            if self.length:
                self.body = barray[self.__class__.st_package.size:self.total]
        finally:
            del barray[:self.total]

    def read_data(self) -> Any:
        if self.data:
            return self.data
        if self.body is None:
            return
        try:
            self.data = msgpack.unpackb(
                self.body,
                use_list=False,
                strict_map_key=False)
        except Exception:
            logging.error(f'failed to unpack: {self}')
            raise
        self.body = None
        return self.data

    def __repr__(self) -> str:
        return '<id: {0.pid} size: {0.length} tp: {0.tp}>'.format(self)
