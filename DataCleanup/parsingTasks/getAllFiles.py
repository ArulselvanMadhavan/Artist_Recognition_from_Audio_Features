__author__ = 'arul'

import os
import glob
import sys

def get_all_files(basedir,ext='.h5') :
    """
    From a root directory, go through all subdirectories
    and find all files with the given extension.
    Return all absolute paths in a list.
    """
    allfiles = []
    for root, dirs, files in os.walk(basedir):
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files :
            allfiles.append(os.path.abspath(f))
            # allfiles.append(os.path.relpath(os.path.abspath(f),basedir))
    return allfiles


if __name__ == '__main__':

    if len(sys.argv)!=3:
        print("Enter the root directory to start searching")
        # Add Code that spits error and exits
    inputDir = sys.argv[1]
    outputDir = sys.argv[2]
    allFiles = get_all_files(sys.argv[1])
    allFiles = sorted(allFiles)
    allFilesAsStringLines = "\n".join(allFiles)
    with open (outputDir,'w+')as outputFileWriter:
        outputFileWriter.write(allFilesAsStringLines)


