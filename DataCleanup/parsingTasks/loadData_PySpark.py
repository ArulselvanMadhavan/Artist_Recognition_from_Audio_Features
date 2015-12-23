__author__ = 'arul'

import hdf5_getters as GETTERS
import sys
import numpy as np
import happybase
# import utils.hbaseConnect as hbase
import logging
import operator
import os
from pyspark import SparkContext

logging.basicConfig(filename='debug.txt',level=logging.DEBUG)
ARTIST_ID_COLUMNID='ArtistId'
COLUMN_FAMILY_NAME='cf'
YEAR_COLUMNID='year'

def getColumnValuesDict(features,h5FileName,artistId,trackId,year):
    """
    Prepare a dictionary that can be assigned as the column values to a row.
    :param features:
    :param h5FileName:
    :param artistId:
    :param trackId:
    :return:
    """
    # featuresDict = dict(enumerate(features))
    featuresDict = {}
    #Assert that features size = 1*90
    for val in range(0,features.shape[1]):
        try:
            featuresDict[COLUMN_FAMILY_NAME+':'+str(val+1)] = str(features[0][val])
        except Exception,e:
            print e
    if (featuresDict.__len__() == 90):
        featuresDict[COLUMN_FAMILY_NAME+':'+str(ARTIST_ID_COLUMNID)] = artistId
        featuresDict[COLUMN_FAMILY_NAME+':'+str(YEAR_COLUMNID)] = str(year)
        return featuresDict
    else:
        logging.debug("Features Dict is not of size 90 for {}",h5FileName)
        return None


def readAndSave(h5File,hBasetableObj):
        h5 = GETTERS.open_h5_file_read(h5File)
        timbreFeatures = GETTERS.get_segments_timbre(h5).T
        ftlen = timbreFeatures.shape[1]
        ndim = timbreFeatures.shape[0]
        assert ndim==12,'WRONG FEATURE DIMENSION, transpose issue?'
        finaldim = 90
        # too small case
        if ftlen < 3:
            #Skip the file as it is not going to be useful
            return None
        avg = np.average(timbreFeatures,1)
        cov = np.cov(timbreFeatures)
        covflat = []
        for k in range(12):
            covflat.extend( np.diag(cov,k) )
        covflat = np.array(covflat)
        timbreFeatures = np.concatenate([avg,covflat])
        features = timbreFeatures.reshape(1,finaldim)
        artistId = GETTERS.get_artist_id(h5)
        trackId = GETTERS.get_track_id(h5)
        year = GETTERS.get_year(h5)
        h5.close()
        return saveDataInHbase(h5File,artistId,trackId,year,features,hBasetableObj)

def saveDataInHbase(h5FileName,artistId, trackId, year,features,hBasetableObj):
    """
    Get the features and save in hbase db.
    Need to change the logic to include batch commits.
    :param artistId:
    :param trackId:
    :param features:
    :return:
    """

    columnsDict = getColumnValuesDict(features,h5FileName,artistId,trackId,year)
    if(columnsDict is not None and columnsDict.__len__() != 0):
        hBasetableObj.put(trackId,columnsDict)
        return True
    else:
        logging.debug("Skipping the record from file {}",h5FileName)
        return False

def readParition(filesIterator):
    """
    Create a database connection
    :param files:
    :return:
    """
    print("I got initialized")
    tableName = 'timbre_sample'
    hbaseCon = createHbaseTable(tableName)
    hBaseTableObj = hbaseCon.table(tableName)
    result = []
    for file in filesIterator:
        result.append(readAndSave(file,hBaseTableObj))
    hbaseCon.close()
    return result
    # files.map(lambda file: readAndSave(file,hBaseTableObj))

COLUMN_FAMILY_NAME='cf'

def createHbaseTable(tableName,host='localhost',port=9090):
    # con = happybase.Connection('ec2-52-91-167-10.compute-1.amazonaws.com',port)
    con = happybase.Connection('localhost',port)
    # if(not con.tables().__contains__(tableName)):
    #     con.create_table(tableName,{
    #         COLUMN_FAMILY_NAME:dict()
    #     })
    #     print "table Created"
    # print con.tables()
    return con

if __name__ == '__main__':
    filePath = sys.argv[1]
    with open(filePath,'r+') as fileList:
        fileListContent = fileList.read()
    listOfFiles = fileListContent.split('\n')
    #Last element is not valid
    listOfFiles.pop()
    sc = SparkContext(appName="msd")
    # print os.path.join(os.getcwd(),"/libraries/happybase.zip")
    # sc.addPyFile(os.path.join(os.getcwd(),"libraries/happybase.zip"))

    sc.addPyFile(sys.argv[2]+"happybase.zip")
    sc.addPyFile(sys.argv[2]+"thrift.zip")
    sc.addPyFile(sys.argv[2]+"hdf5_getters.py")
    sc.addPyFile(sys.argv[2]+"tables.egg")

    # filesRDD = sc.textFile(filePath)
    filesRDD = sc.parallelize(listOfFiles)
    resultList = filesRDD.mapPartitions(readParition).collect()
    finalResult = reduce(operator.and_,resultList,True)
    print finalResult
    # try:
    #     for file in listOfFiles[1:]:
    #         readAndSave(file)
    # except Exception,e:
    #     logging.debug("Save failed for {}",file)

