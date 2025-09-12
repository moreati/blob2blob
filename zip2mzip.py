#!/usr/bin/env python3

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "multiformats",
# ]
# ///

import sys

import zipfile_blob

with (
    zipfile_blob.ZipFile(sys.stdin.buffer) as infile,
    #sys.stdout.buffer as outfile,
):
    for info in sorted(infile.infolist(), key=lambda info: info.header_offset):
        with infile.open(info) as f:
            lf_header = info._local_file_header

            lf_start = lf_header._lf_location
            lf_end= lf_header._lf_location + len(lf_header._lf_data) - 1

            if info.compress_size:
                data_start = lf_end + 1
                data_end = lf_end + info.compress_size
            else:
                data_start = data_end = lf_end

            print(f'LF{lf_start:>8}{lf_end:>8}  PL{data_start:>8}{data_end:>8}')
    for info in infile.infolist():
        cd_entry = info._central_directory_entry

        cd_start = cd_entry._cde_location
        cd_end = cd_start + len(cd_entry._cde_data) - 1

        print(f'CD{cd_start:>8}{cd_end:>8}')


    ed_start = infile._endrec._ecd_location
    ed_end = ed_start + len(infile._endrec._ecd_content) - 1
    print(f'ED{ed_start:>8}{ed_end:>8}')

    # TODO ED64
    # TODO EDL64
