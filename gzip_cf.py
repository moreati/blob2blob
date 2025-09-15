#!/usr/bin/env python3
'''
Gzip for control freaks, explicitly specify compression and metadata

$ printf '' | ./gzip_cf.py --level 9 --method DEFLATED --operating-system UNIX --mtime 0 > empty-cf.gz
$ printf '' | python -m gzip --best > empty-py.gz
$ printf '' | /usr/bin/gzip -9 -n > empty.gz
$ sha256sum empty*.gz
f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec  empty-cf.gz
6e2a87d8bd8878d952c5c304bca5ada9ec5a54d656b9f87bb5ebef72177fd5a7  empty-py.gz
f61f27bd17de546264aa58f40f3aafaac7021e0ef69c17f6b1b4cd7664a037ec  empty.gz

$ cat Python-3.14.0rc2.tar | ./gzip_cf.py --level 9 --method DEFLATED --operating-system UNIX --mtime 0 > tar-cf.gz
$ cat Python-3.14.0rc2.tar | python -m gzip --best > tar-py.gz
$ cat Python-3.14.0rc2.tar | /usr/bin/gzip -9 -n > tar.gz
$ sha256sum tar*.gz
cda8b1511b0ab4346333aab05da9f1e8f97c1ef919134e0cce6a8ef819cd11cb  tar-cf.gz
ac6784a7be1409ec89900fc36b14ae95f3a0687134150c002d03f2ccd4a0703a  tar-py.gz
cda8b1511b0ab4346333aab05da9f1e8f97c1ef919134e0cce6a8ef819cd11cb  tar.gz
'''

import dataclasses
import enum
import io
import struct
import zlib

NUL = b'\x00'
COMPRESS_LEVELS = (1, 2, 3, 4, 5, 6, 7, 8, 9)


class _EnumArgMixin:
    @classmethod
    def _choices(cls):
        return [e.name.lower() for e in cls]

    @classmethod
    def _from_arg(cls, arg: str):
        return cls[arg.upper()]


class CompressionMethod(_EnumArgMixin, enum.IntEnum):
    DEFLATED = 8


class Flags(enum.IntFlag):
    TEXT = enum.auto()
    HEADER_CRC = enum.auto()
    EXTRA_FIELDS = enum.auto()
    FILE_NAME = enum.auto()
    COMMENT = enum.auto()


class ExtraFlags(enum.IntEnum):
    pass


class ExtraDeflateFlags(ExtraFlags):
    MAXIMUM_COMPRESSION = 2
    FASTEST_COMPRESSION = 4


class OperatingSystem(_EnumArgMixin, enum.IntEnum):
    FAT = 0
    AMIGA = 1
    VMS = 2
    UNIX = 3
    VM_CMS = 4
    ATARI_TOS = 5
    HPFS = 6
    MACINTOSH = 7
    Z_SYSTEM = 8
    CP_M = 9
    TOPS_20 = 10
    NTFS = 11
    QDOS = 12
    RISCOS = 13
    UNKNOWN = 255


@dataclasses.dataclass(frozen=True, slots=True)
class Options:
    method: CompressionMethod
    flags: Flags
    modification_time: int
    extra_flags: ExtraFlags
    operating_system: OperatingSystem
    extra_length: int
    extra_field: bytes
    original_file_name: bytes
    comment: bytes
    header_crc: int


def compress(
    data: bytes,
    level: int,
    compression_method: CompressionMethod,
    is_text: bool,
    operating_system: OperatingSystem,
    extra_field: None,
    file_mtime: int,
    file_name: bytes | None,
    comment: bytes | None,
    header_crc: bool,
):
    assert level in COMPRESS_LEVELS
    assert compression_method in CompressionMethod
    assert operating_system in OperatingSystem
    assert extra_field is None

    f = io.BytesIO()

    flags = 0
    if is_text:
        flags |= Flags.TEXT
    if header_crc:
        flags |= Flags.HEADER_CRC
    if file_name is not None:
        assert NUL not in file_name
        flags |= Flags.FILE_NAME
    if comment is not None:
        assert NUL not in comment
        flags |= Flags.COMMENT

    match level:
        case 1: extra_flags = ExtraDeflateFlags.FASTEST_COMPRESSION
        case 9: extra_flags = ExtraDeflateFlags.MAXIMUM_COMPRESSION
        case _: extra_flags = 0

    # TODO Is MTIME signed or unsigned? RFC1952 & Python gzip seem to disagree
    f.write(struct.pack('<2sBBLBB', b'\x1f\x8b', compression_method, flags,
                        file_mtime, extra_flags, operating_system))

    if file_name is not None:
        f.write(file_name)
        f.write(NUL)

    if comment is not None:
        f.write(comment)
        f.write(NUL)

    if header_crc:
        crc16 = zlib.crc32(f.getvalue()) & 0x0000ffff
        f.write(struct.pack('<H', crc16))

    wbits = -15  # 15 -> Maximum window size; -ive -> raw deflate stream
    compressor = zlib.compressobj(level, compression_method, wbits)
    f.write(compressor.compress(data))
    f.write(compressor.flush())
    crc32 = zlib.crc32(data)
    size = len(data) % 2**32
    f.write(struct.pack('<LL', crc32, size))

    return f.getvalue()


if __name__ == '__main__':
    import argparse
    import sys

    p = argparse.ArgumentParser(description = str(__doc__).split('\n')[0])
    p.add_argument('--level', metavar='{1..9}', type=int, choices=COMPRESS_LEVELS, required=True)
    p.add_argument('--method', type=CompressionMethod._from_arg, choices=CompressionMethod, required=True)
    p.add_argument('--operating-system', type=OperatingSystem._from_arg, choices=OperatingSystem, required=True)
    p.add_argument('--mtime', type=int, required=True)
    p.add_argument('--is-text', action='store_true')
    p.add_argument('--header-crc', action='store_true')
    p.add_argument('--name')
    p.add_argument('--comment')

    args = p.parse_args()
    if args.name is not None:
        args.name = args.name.encode('iso8859-1')
    if args.comment is not None:
        args.comment = args.comment.encode('iso8859-1')

    data_in  = sys.stdin.buffer.read()
    sys.stdout.buffer.write(compress(
        data_in, args.level, args.method.value, args.is_text,
        args.operating_system, None, args.mtime, args.name, args.comment,
        args.header_crc,
    ))
