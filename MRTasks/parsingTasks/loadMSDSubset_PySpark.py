__author__ = 'ameyapandilwar'

import sys
import operator
import happybase
import hdf5_getters as GETTERS
from pyspark import SparkContext

COLUMN_FAMILY_NAME = 'cf'
ARTIST_HOTTTNESSS_COLUMNID = 'artist_hotttnesss'
ARTIST_ID_COLUMNID = 'artist_id'
ARTIST_NAME_COLUMNID = 'artist_name'
DANCEABILITY_COLUMNID = 'danceability'
RELEASE_COLUMNID = 'release'
SONG_HOTTTNESSS_COLUMNID = 'song_hotttnesss'
SONG_ID_COLUMNID = 'song_id'
TEMPO_COLUMNID = 'tempo'
TITLE_COLUMNID = 'title'
TRACK_ID_COLUMNID = 'track_id'
YEAR_COLUMNID = 'year'


def getColumnValuesDict(artistHotttnesss, artistId, artistName, danceability, release, songHotttnesss,
                        songId, tempo, title, trackId, year):
    features_dict = {
        COLUMN_FAMILY_NAME + ':' + str(ARTIST_HOTTTNESSS_COLUMNID): str(artistHotttnesss),
        COLUMN_FAMILY_NAME + ':' + str(ARTIST_ID_COLUMNID): artistId,
        COLUMN_FAMILY_NAME + ':' + str(ARTIST_NAME_COLUMNID): artistName,
        COLUMN_FAMILY_NAME + ':' + str(DANCEABILITY_COLUMNID): str(danceability),
        COLUMN_FAMILY_NAME + ':' + str(RELEASE_COLUMNID): release,
        COLUMN_FAMILY_NAME + ':' + str(SONG_HOTTTNESSS_COLUMNID): str(songHotttnesss),
        COLUMN_FAMILY_NAME + ':' + str(SONG_ID_COLUMNID): songId,
        COLUMN_FAMILY_NAME + ':' + str(TEMPO_COLUMNID): str(tempo),
        COLUMN_FAMILY_NAME + ':' + str(TITLE_COLUMNID): title,
        COLUMN_FAMILY_NAME + ':' + str(TRACK_ID_COLUMNID): trackId,
        COLUMN_FAMILY_NAME + ':' + str(YEAR_COLUMNID): str(year)
    }

    return features_dict


def read_and_save(h5File, hBasetableObj):
    h5 = GETTERS.open_h5_file_read(h5File)

    artistHotttnesss = GETTERS.get_artist_hotttnesss(h5)
    artistId = GETTERS.get_artist_id(h5)
    artistName = GETTERS.get_artist_name(h5)
    danceability = GETTERS.get_danceability(h5)
    release = GETTERS.get_release(h5)
    songHotttnesss = GETTERS.get_song_hotttnesss(h5)
    songId = GETTERS.get_song_id(h5)
    tempo = GETTERS.get_tempo(h5)
    title = GETTERS.get_title(h5)
    trackId = GETTERS.get_track_id(h5)
    year = GETTERS.get_year(h5)

    h5.close()
    return saveDataInHbase(h5File, artistHotttnesss, artistId, artistName, danceability, release, songHotttnesss,
                           songId, tempo, title, trackId, year, hBasetableObj)


def saveDataInHbase(h5FileName, artistHotttnesss, artistId, artistName, danceability, release, songHotttnesss,
                    songId, tempo, title, trackId, year, hBasetableObj):
    columnsDict = getColumnValuesDict(artistHotttnesss, artistId, artistName, danceability, release, songHotttnesss,
                                      songId, tempo, title, trackId, year)
    if columnsDict is not None and len(columnsDict) != 0:
        hBasetableObj.put(trackId, columnsDict)
        return True
    else:
        return False


def read_parition(files_iterator):
    print("system has initialized . . . ")
    tableName = 'msd_subset'
    hbaseCon = create_hbase_table(tableName)
    hBaseTableObj = hbaseCon.table(tableName)
    result = []
    for file in files_iterator:
        result.append(read_and_save(file, hBaseTableObj))
    hbaseCon.close()
    return result


def create_hbase_table(tableName, host='localhost', port=9090):
    con = happybase.Connection('localhost', port)
    return con


if __name__ == '__main__':
    with open(sys.argv[1], 'r+') as fileList:
        fileListContent = fileList.read()

    listOfFiles = fileListContent.split('\n')
    listOfFiles.pop()

    sc = SparkContext(appName="msd")
    sc.addPyFile(sys.argv[2] + "happybase.zip")
    sc.addPyFile(sys.argv[2] + "thrift.zip")
    sc.addPyFile(sys.argv[2] + "hdf5_getters.py")
    sc.addPyFile(sys.argv[2] + "tables.egg")

    filesRDD = sc.parallelize(listOfFiles)
    resultList = filesRDD.mapPartitions(read_parition).collect()
    finalResult = reduce(operator.and_, resultList, True)

    print finalResult