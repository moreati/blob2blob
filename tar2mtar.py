#!/usr/bin/env python3

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "multiformats",
# ]
# ///
"""
Convert a tar file (read from stdin) to an mtar (written to stdout).

Mtar is a prototype file archive format based on tar. It preserves metadata
of contained files (the header blocks), but replaces the data with a
reference to the same data held and de-duplicated in Content Addressable
Storage (CAS).

Given an MTar and the CAS one should be able to reconstitute a tar that is
bit-for-bit identical to the original. So file1.tar -> mtar -> file2.tar2
should result in two tar files with the same digest (e.g. sha256sum).

An mtar is composed of 1 mtar header block followed by tar header blocks.
Like tar each block is 512 bytes and compression is handled externally.
"""

import hashlib
import os
import struct
import sys

import multiformats

import tarfile_blob

# 2 bytes to prevent detection as ASCII/UTF-8/UTF-16
# 4 bytes of arbitraryily chosen ASCII format identifier
# 2 bytes of little-endian format version
# See https://hackers.town/@zwol/114155807716413069
MTAR_MAGIC = struct.pack(">2s4sH", b"\xdc\xdf", b"MTAR", 1)

TAR_LINK_EMPTY = tarfile_blob.NUL * tarfile_blob.LENGTH_LINK
TAR_LINK_OFFSET = 157

with (
    tarfile_blob.open(fileobj=sys.stdin.buffer, mode="r|") as tf,
    sys.stdout.buffer as outf,
):
    # A full 512 bytes for the header, even though it's just 8 bytes.
    # Seems a good idea to keep alignment, but happy to do otherwise if
    # costs outweight benefits.
    outf.write(MTAR_MAGIC.ljust(tarfile_blob.BLOCKSIZE, tarfile_blob.NUL))

    for ti in tf:
        buf = ti.header
        assert len(buf) >= tarfile_blob.BLOCKSIZE
        assert len(buf) % tarfile_blob.BLOCKSIZE == 0, (
            f"Unexpected header size: {len(buf)}"
        )

        # For anything that isn't a regular file write the header unmodified
        if not ti.isfile():
            outf.write(buf)
            continue

        # MTAR reuses the link field to store a hash of the file content
        # present in the source tar. This relies on a) link field being empty
        # b) link field being fully zeroed. If either is not true then abort.
        assert (
            buf[TAR_LINK_OFFSET : TAR_LINK_OFFSET + tarfile_blob.LENGTH_LINK]
            == TAR_LINK_EMPTY
        )

        # Unknown entry type that isn't a regular file, but it has file data.
        # Abort to avoid silently creating invalid output.
        mf = tf.extractfile(ti)
        if not mf:
            raise ValueError(f"Expected file content from member: {ti}")

        # Regular file entry - save the content to Content Addressable Storage
        # and write an identifier to the output MTAR. Uses an Interplanetary
        # Filesystem Content Identifier (IPFS CID) as the identifier.
        with (
            mf,
            open(os.devnull, "wb") as blobf,  # FIXME Actually use a CAS
        ):
            hasher = hashlib.sha256()
            while chunk := mf.read(tarfile_blob.RECORDSIZE):
                blobf.write(chunk)
                hasher.update(chunk)
        mhash = multiformats.multihash.wrap(hasher.digest(), "sha2-256")
        cid = multiformats.CID("base32", 1, "raw", mhash)
        cid_b = str(cid).encode("ascii")
        assert len(cid_b) < tarfile_blob.LENGTH_LINK
        cid_nts = cid_b.ljust(tarfile_blob.LENGTH_LINK, tarfile_blob.NUL)
        buf[TAR_LINK_OFFSET : TAR_LINK_OFFSET + tarfile_blob.LENGTH_LINK] = cid_nts

        outf.write(buf)
