#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "backports-zstd; python_version < '3.14'",
# ]
# ///
"""
Write a directory tree of compression examples with metadata
"""

import abc
import argparse
import json
import gzip
import hashlib
import io
import lzma
import os
import pathlib
import platform
import subprocess
import sys
import tempfile
import types
import typing
import zlib

try:
    import compression.zstd as zstd
except ImportError:
    import backports.zstd as zstd

INPUTS = {
    '0-empty': lambda: b'',
    '1-a': lambda: b'a',
    '1e1-a': lambda: b'a' * 10,
    '1e2-a': lambda: b'a' * 100,
    '1e3-a': lambda: b'a' * 1000,
    '1e6-a': lambda: b'a' * 1000_000,
    '1e8-a': lambda: b'a' * 100_000_000,
}

INFGEN_ARGS = [
    '-dd',  # Write bit pattern of each element
    '-i',  # Write gzip or zlib header info
]


class BaseCmd(abc.ABC):
    common_args: list[str]
    variants: dict[str, list[str]]
    extension: str
    format: str

    def __init__(self, cmd: str):
        self.cmd = cmd

    def version(self):
        res = subprocess.run(
            [self.cmd, '--version'],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding='ascii',
            check=True,
        )
        if res.stdout:
            return res.stdout
        return res.stderr

    def compress_file(self, filename: os.PathLike, variant: str):
        result = subprocess.run(
            [self.cmd, *self.common_args, *self.variants[variant], filename],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            encoding='ascii',
            check=True,
        )
        out_file_path = pathlib.Path(f'{filename}.{self.extension}')
        compressed_data = out_file_path.read_bytes()
        out_file_path.unlink()

        filename_placeholder = '<filename>'
        info = dict(
            args=result.args[:-1] + [filename_placeholder],
            stdout=result.stdout,
            stderr=result.stderr.replace(str(filename), filename_placeholder),
            returncode=result.returncode,
        )
        return compressed_data, info

    def compress_stdio(self, data: bytes, variant: str):
        result = subprocess.run(
            [self.cmd, *self.common_args, *self.variants[variant]],
            input=data,
            capture_output=True,
            check=True,
        )
        info = dict(
            args=result.args,
            stderr=result.stderr.decode('ascii'),
            returncode=result.returncode,
        )
        return result.stdout, info


class GzipCmd(BaseCmd):
    common_args = [
        '--keep',
        '--no-name',  # Suppress filename & timestamp in header
    ]
    variants = {
        'fastest': ['-1'],
        'default': [],
        'best': ['-9'],
    }
    extension = 'gz'
    format = 'gzip'


class XzCmd(BaseCmd):
    common_args = [
        '--keep',
    ]
    variants = {
        'mt1-fastest': ['-0', '--threads=+1'],
        'mt1-default': ['--threads=+1'],
        'mt1-best': ['-9', '--threads=+1'],
        'mt1-best-extreme': ['-9e', '--threads=+1'],
        'mt2-fastest': ['-0', '--threads=+1'],
        'mt2-default': ['--threads=2'],
        'mt2-best': ['-9', '--threads=2'],
        'mt2-best-extreme': ['-9e', '--threads=2'],
        'st-fastest': ['-0', '--threads=1'],
        'st-default': ['--threads=1'],
        'st-best': ['-9', '--threads=1'],
        'st-best-extreme': ['-9e', '--threads=1'],
    }
    extension = 'xz'
    format = 'xz'


class ZstdCmd(BaseCmd):
    common_args = [
        '--no-progress',
    ]
    variants = {
        'mt1-fastest': ['-1', '--threads=1'],
        'mt1-default': ['-3', '--threads=1'],
        'mt1-best': ['-19', '--threads=1'],
        'mt1-best-ultra': ['--ultra', '-22', '--threads=1'],
        'mt2-fastest': ['-1', '--threads=2'],
        'mt2-default': ['-3', '--threads=2'],
        'mt2-best': ['-19', '--threads=2'],
        'mt2-best-ultra': ['--ultra', '-22', '--threads=2'],
        'st-fastest': ['-1', '--single-thread'],
        'st-default': ['-3', '--single-thread'],
        'st-best': ['-19', '--single-thread'],
        'st-best-ultra': ['--ultra', '-22', '--single-thread'],
    }
    extension = 'zst'
    format = 'zstd'


class BaseLib(abc.ABC):
    common_args: dict[str, typing.Any]
    variants: dict[str, dict[str, typing.Any]]
    extension: str
    format: str

    def __init__(
            self,
            mod: types.ModuleType,
            compress_callable,
            compressor_callable=None,
    ):
        self.mod = mod
        self.compress_callable = compress_callable
        self.compressor_callable = compressor_callable

    def combined_args(self, variant: str):
        return self.common_args | self.variants[variant]

    def compress(self, data: bytes, variant: str):
        args = self.combined_args(variant)
        compressed_data = self.compress_callable(data, **args)
        info = {'args': args, 'module': self.mod.__name__}
        return compressed_data, info

    def compressor_compress(self, chunks: list[bytes], variant: str):
        assert self.compressor_callable is not None
        args = self.combined_args(variant)
        compressor = self.compressor_callable(**args)
        compressed_pieces = [compressor.compress(chunk) for chunk in chunks]
        compressed_pieces.append(compressor.flush())
        info = {'args': args, 'compressor': type(compressor).__name__}
        return b''.join(compressed_pieces), info


class GzipLib(BaseLib):
    common_args = {'mtime': 0}
    variants = {
        'fastest': {'compresslevel': 1},
        'default': {},
        'best': {'compresslevel': 9},
    }
    extension = 'gz'
    format = 'gzip'


class XzLib(BaseLib):
    common_args = {}
    variants = {
        'st-fastest': {'preset': 0},
        'st-default': {},
        'st-best': {'preset': 9},
        'st-best-extreme': {'preset': 9 | lzma.PRESET_EXTREME},
    }
    extension = 'xz'
    format = 'xz'


class ZstdLib(BaseLib):
    common_args = {
        'options': {
            'checksum_flag': True,
        },
    }
    variants = {
        'mt1-fastest': {'options': {'compression_level': 1, 'nb_workers': 1}},
        'mt1-default': {'options': {'compression_level': 3, 'nb_workers': 1}},
        'mt1-best': {'options': {'compression_level': 19, 'nb_workers': 1}},
        'mt1-best-ultra': {'options': {'compression_level': 22, 'nb_workers': 1}},

        'mt2-fastest': {'options': {'compression_level': 1, 'nb_workers': 2}},
        'mt2-default': {'options': {'compression_level': 3, 'nb_workers': 2}},
        'mt2-best': {'options': {'compression_level': 19, 'nb_workers': 2}},
        'mt2-best-ultra': {'options': {'compression_level': 22, 'nb_workers': 2}},

        'st-fastest': {'options': {'compression_level': 1, 'nb_workers': 0}},
        'st-default': {'options': {'compression_level': 3, 'nb_workers': 0}},
        'st-best': {'options': {'compression_level': 19, 'nb_workers': 0}},
        'st-best-ultra': {'options': {'compression_level': 22, 'nb_workers': 0}},
    }
    extension = 'zst'
    format = 'zstd'

    def __init__(
            self,
            mod: types.ModuleType,
            compress_callable,
            compressor_callable=None,
    ):
        super().__init__(mod, compress_callable, compressor_callable)
        self._fixup_options(self.common_args)
        for args in self.variants.values():
            self._fixup_options(args)

    def _fixup_options(self, args):
        try:
            args['options']
        except KeyError:
            return
        args['options'] = {
            getattr(zstd.CompressionParameter, k): v
            for k, v in args['options'].items()
        }

    def combined_args(self, variant: str):
        args = super().combined_args(variant)
        options = self.common_args.get('options', {}).copy()
        options.update(self.variants[variant].get('options', {}))
        args['options'] = options
        return args


def environment_info():
    info = {}
    uname = platform.uname()
    info.update(
        platform=dict(
            machine=platform.machine(),
            release=platform.release(),
            system=platform.system(),
            uname=dict(
                machine=uname.machine,
                processor=uname.processor,
                release=uname.release,
                system=uname.system,
                version=uname.version,
            ),
            version=platform.version(),
        ),
        python=dict(
            stdlib=dict(
                sys=dict(
                    maxsize=sys.maxsize,
                    version=sys.version,
                    version_info=sys.version_info,
                ),
                zlib=dict(
                    ZLIB_VERSION=zlib.ZLIB_VERSION,
                    ZLIB_RUNTIME_VERSION=zlib.ZLIB_RUNTIME_VERSION,
                ),
                zstd=dict(
                    COMPRESSION_LEVEL_DEFAULT=zstd.COMPRESSION_LEVEL_DEFAULT,
                    zstd_version_info=zstd.zstd_version_info,
                ),
            ),

        ),
    )
    return info


def infgen(infgen_cmd: str, gzip_data: bytes):
    return subprocess.run(
        [infgen_cmd] + INFGEN_ARGS,
        input=gzip_data,
        capture_output=True,
        check=True,
    )


def write_example(impl, out_dir: os.PathLike, name: str, data: bytes, info: dict) -> tuple[pathlib.Path, pathlib.Path]:
    out_dir = pathlib.Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    data_path = out_dir / f'{name}.{impl.extension}'
    data_path.write_bytes(data)
    info_path = out_dir / f'{name}.json'
    info_path.write_text(json.dumps(info, indent=2, sort_keys=True))
    return data_path, info_path


def file_sha256(path: os.PathLike):
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(io.DEFAULT_BUFFER_SIZE):
            hasher.update(chunk)
    return hasher


def write_sha256sums(out_dir: os.PathLike, relative_names: list[os.PathLike]):
    out_dir = pathlib.Path(out_dir)
    hashers = [file_sha256(out_dir / name) for name in relative_names]
    with open(out_dir / 'sha256sums', 'w', encoding='utf-8') as f:
        for name, hasher in zip(relative_names, hashers):
            f.write(f'{hasher.hexdigest()} *{name}\n')


def main():
    if sys.version_info < (3, 13):
        sys.exit('Python 3.13 or higher is required.')

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group = parser.add_argument_group('commands')
    group.add_argument(
        '--gzip-cmd',
        default='gzip',
        type=GzipCmd,
        metavar='CMD',
    )
    group.add_argument('--infgen-cmd', default='infgen', metavar='CMD')
    group.add_argument(
        '--xz-cmd',
        default='xz',
        type=XzCmd,
        metavar='CMD',
    )
    group.add_argument(
        '--zstd-cmd',
        default='zstd',
        type=ZstdCmd,
        metavar='CMD',
    )

    parser.add_argument(
        '--output-dir',
        default='examples/generated',
        metavar='PATH',
        type=pathlib.Path,
        help='Directory to write output to',
    )
    # parser.add_argument("name", help="Name to give this set of examples")
    args = parser.parse_args()

    output_base: pathlib.Path = args.output_dir
    output_base.mkdir(parents=True, exist_ok=True)

    env_info_path = output_base / 'environment-info.json'
    env_info_path.write_text(
        json.dumps(environment_info(), indent=2, sort_keys=True),
    )

    inputs = {name: fn() for name, fn in INPUTS.items()}
    cmds: list[BaseCmd] = [
        args.gzip_cmd,
        args.xz_cmd,
        args.zstd_cmd,
    ]
    libs: list[BaseLib] = [
        # Python's stdlib gzip doesn't have a compressor class
        GzipLib(gzip, gzip.compress, None),
        XzLib(lzma, lzma.compress, lzma.LZMACompressor),
        ZstdLib(zstd, zstd.compress, zstd.ZstdCompressor),
    ]

    # cmd.compress_file() examples, uncompressed data provided in a named file
    with tempfile.TemporaryDirectory() as tmpdir:
        for in_name, in_data in inputs.items():
            in_path = pathlib.Path(tmpdir, in_name)
            in_path.write_bytes(in_data)

        for cmd in cmds:
            for variant in cmd.variants:
                out_dir = output_base / cmd.format / 'cmd_file' / variant
                data_paths: list[pathlib.Path] = []
                for in_name in inputs:
                    in_path = pathlib.Path(tmpdir, in_name)
                    out_data, out_info = cmd.compress_file(in_path, variant)
                    data_path, _ = write_example(cmd, out_dir, in_name, out_data, out_info)
                    data_paths.append(data_path)
                write_sha256sums(out_dir, [p.name for p in data_paths])

    # cmd.compress_stdio() examples, uncompressed data provided to stdin
    for cmd in cmds:
        for variant in cmd.variants:
            out_dir = output_base / cmd.format / 'cmd_stdio' / variant
            data_paths: list[pathlib.Path] = []
            for in_name, in_data in inputs.items():
                out_data, out_info = cmd.compress_stdio(in_data, variant)
                data_path, _  = write_example(cmd, out_dir, in_name, out_data, out_info)
                data_paths.append(data_path)
            write_sha256sums(out_dir, [p.name for p in data_paths])

    # lib.compress() examples, uncompressed data provided as a byte string
    for lib in libs:
        for variant in lib.variants:
            out_dir = output_base / lib.format / 'py_stdlib_compress' / variant
            data_paths: list[pathlib.Path] = []
            for in_name, in_data in inputs.items():
                out_data, out_info = lib.compress(in_data, variant)
                data_path, _  = write_example(lib, out_dir, in_name, out_data, out_info)
                data_paths.append(data_path)
            write_sha256sums(out_dir, [p.name for p in data_paths])

    # lib.*Compressor() examples, uncompressed data provided as an interator of byte strings
    for lib in libs:
        if lib.compressor_callable is None:
            continue
        for variant in lib.variants:
            out_dir = output_base / lib.format / 'py_stdlib_compressor' / variant
            data_paths: list[pathlib.Path] = []
            for in_name, in_data in inputs.items():
                chunks = (in_data[i:1024+i] for i in range(0, len(in_data), 1024))
                out_data, out_info = lib.compressor_compress(chunks, variant)
                data_path, _  = write_example(lib, out_dir, in_name, out_data, out_info)
                data_paths.append(data_path)
            write_sha256sums(out_dir, [p.name for p in data_paths])

if __name__ == '__main__':
    main()
