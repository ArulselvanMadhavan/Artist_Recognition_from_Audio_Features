__author__ = 'arul'

import hdf5_getters as GETTERS
import sys
import numpy as np
import happybase
import utils.hbaseConnect as hbase
import logging

logging.basicConfig(filename='debug.txt',level=logging.DEBUG)
ARTIST_ID_COLUMNID=91

def getColumnValuesDict(features,h5FileName,artistId,trackId):
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
            featuresDict[hbase.COLUMN_FAMILY_NAME+':'+str(val+1)] = str(features[0][val])
        except Exception,e:
            print e
    if (featuresDict.__len__() == 90):
        featuresDict[hbase.COLUMN_FAMILY_NAME+':'+str(ARTIST_ID_COLUMNID)] = artistId
        return featuresDict
    else:
        logging.debug("Features Dict is not of size 90 for {}",h5FileName)
        return None


class hbaseConnection(object):
    def __init__(self,tableName):
        self.tableName = tableName
        self.hbaseConnection = hbase.createHbaseTable(tableName)


    def saveDataInHbase(self,h5FileName,artistId, trackId, features):
        """
        Get the features and save in hbase db.
        Need to change the logic to include batch commits.
        :param artistId:
        :param trackId:
        :param features:
        :return:
        """
        table = self.hbaseConnection.table(self.tableName)
        columnsDict = getColumnValuesDict(features,h5FileName,artistId,trackId)
        if(columnsDict is not None and columnsDict.__len__() != 0):
            table.put(trackId,columnsDict)
        else:
            logging.debug("Skipping the record from file {}",h5FileName)


    def readAndSave(self,h5File):
        #Get H5 Content
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
        h5.close()
        self.saveDataInHbase(h5File,artistId,trackId,features)

if __name__ == '__main__':
    filePath = sys.argv[1]
    with open(filePath,'r+') as fileList:
        fileListContent = fileList.read()
    listOfFiles = fileListContent.split('\n')
    hbs = hbaseConnection("test")
    try:
        for file in listOfFiles[1:]:
            hbs.readAndSave(file)
    except Exception,e:
        logging.debug("Save failed for {}",file)

