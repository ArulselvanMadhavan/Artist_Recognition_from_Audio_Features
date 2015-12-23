import sys

__author__ = 'arul'

# import os
# import zipfile
#
# def zipdir(path, ziph):
#     # ziph is zipfile handle
#     for root, dirs, files in os.walk(path):
#         for file in files:
#             ziph.write(os.path.join(root, file))
#
# if __name__ == '__main__':
#     inputDir = sys.argv[1]
#     outputFile = sys.argv[2]
#     zipf = zipfile.ZipFile(outputFile, 'w')
#     zipdir(inputDir, zipf)
#     zipf.close()

from contextlib import closing
from zipfile import ZipFile, ZIP_DEFLATED
import os

def zipdir(basedir, archivename):
    assert os.path.isdir(basedir)
    with closing(ZipFile(archivename, "w", ZIP_DEFLATED)) as z:
        for root, dirs, files in os.walk(basedir):
            #NOTE: ignore empty directories
            for fn in files:
                absfn = os.path.join(root, fn)
                zfn = absfn[len(basedir)+len(os.sep):] #XXX: relative path
                z.write(absfn, zfn)

if __name__ == '__main__':
    import sys
    basedir = sys.argv[1]
    archivename = sys.argv[2]
    zipdir(basedir, archivename)