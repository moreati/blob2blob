blob2blob
=========

A playground for techniques, scripts, file formats that allow recreating
software packages (e.g. '.deb', '.rpm', '.tar.gz', '.tar.xz', '.whl', '.zip')
bit for bit from similar files and small delta descriptions.

For example
- Given linux-firmware_20221212.git0707b2f2-0ubuntu1_all.deb can
  linux-firmware_20230120.gitbb2d42dc-0ubuntu1_all.deb be obtained without
  downloading all 278 MiB again.
- Can tensorflow-2.19.1-cp312-cp312-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
  and tensorflow-2.20.0-cp313-cp313-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
  be served without storing a complete copy of both?


Prototypes
----------

`tar2mtar.py`
    Strips file data from an uncompressed .tar file and produces a .mtar file
    containing the metadata plus IPFS CIDs identifying the stripped content.

`zip2mzip.py`
    WIP. WIll perform the same stripping to a .zip file, producing a .mzip.


Related projects and reading
----------------------------

- https://www.chromium.org/developers/design-documents/software-updates-courgette/
- https://github.com/google/grittibanzli
- https://github.com/pah/pristine-tar
- https://reproducible-builds.org/
- https://pypi.org/project/repro-tarfile/
- https://pypi.org/project/repro-zipfile/
- https://pypi.org/project/tarpatch/
