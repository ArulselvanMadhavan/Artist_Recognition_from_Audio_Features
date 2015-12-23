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

ARTIST_FAMILIARITY_COLUMNID = "artist_familiarity"
ARTIST_LOCATION_COLUMNID = "artist_location"
DURATION_COLUMNID = "duration"
ENERGY_COLUMNID = "energy"
LOUDNESS_COLUMNID = "loudness"


def read_and_save(h5File):
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

    artistFamiliarity = GETTERS.get_artist_familiarity(h5)
    artistLocation = GETTERS.get_artist_location(h5)
    duration = GETTERS.get_duration(h5)
    energy = GETTERS.get_energy(h5)
    loudness = GETTERS.get_loudness(h5)

    h5.close()
    return saveDataInHbase(h5File, artistHotttnesss, artistId, artistName, danceability, release, songHotttnesss,
                           songId, tempo, title, trackId, year, artistFamiliarity, artistLocation, duration, energy,
                           loudness)


def saveDataInHbase(h5FileName, artistHotttnesss, artistId, artistName, danceability, release, songHotttnesss,
                    songId, tempo, title, trackId, year, artistFamiliarity, artistLocation, duration, energy,
                    loudness):
    result = str(artistHotttnesss) + "\t" + artistId + "\t" + artistName + "\t" + str(danceability) + "\t"\
             + release + "\t" + str(songHotttnesss) + "\t" + songId + "\t" + str(tempo) + "\t" + title + "\t"\
             + trackId + "\t" + str(year) + "\t" + str(artistFamiliarity) + "\t" + artistLocation + "\t"\
             + str(duration) + "\t" + str(energy) + "\t" + str(loudness) + "\n"
    return result


def read_parition(files_iterator):
    print("system has initialized . . . ")
    result = []

    for file in files_iterator:
        result.append(read_and_save(file))
    return result


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

    with open(sys.argv[3], "w+") as fileWriter:
        fileWriter.write("".join(resultList))