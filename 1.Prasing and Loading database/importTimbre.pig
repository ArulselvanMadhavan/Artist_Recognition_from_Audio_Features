-- Pig program to import timbre data from a tab separated file  
-- @ Author - Yogendra Miraje
-- Date: Nov. 27, 2015

 -- Change the location from which you want to import the data
raw = LOAD '/Users/Yogi/Study/3MapReduce/50_Project/50_workspace/pig/backup/timbre/part-m-00000' USING PigStorage('\t') AS 
(id,ArtistId,year,val1,val2,val3,val4,val5,val6,val7,val8,val9,val10,val11,val12,val13,val14,val15,val16,val17,val18,val19,val20,val21,val22,val23,val24,val25,
val26,val27,val28,val29,val30,val31,val32,val33,val34,val35,val36,val37,val38,val39,val40,val41,val42,val43,val44,val45,val46,val47,val48,val49,val50,val51,val52,
val53,val54,val55,val56,val57,val58,val59,val60,val61,val62,val63,val64,val65,val66,val67,val68,val69,val70,val71,val72,val73,val74,val75,val76,val77,val78,val79,
val80,val81,val82,val83,val84,val85,val86,val87,val88,val89,val90);

STORE raw INTO 'hbase://timbre' 
USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:ArtistId cf:year cf:1 cf:2 cf:3 cf:4 cf:5 cf:6 cf:7 cf:8 cf:9 cf:10 cf:11 cf:12 cf:13 cf:14 cf:15 cf:16 cf:17 cf:18 cf:19 cf:20 cf:21 cf:22 cf:23 cf:24 cf:25 cf:26 cf:27 cf:28 cf:29 cf:30 cf:31 cf:32 cf:33 cf:34 cf:35 cf:36 cf:37 cf:38 cf:39 cf:40 cf:41 cf:42 cf:43 cf:44 cf:45 cf:46 cf:47 cf:48 cf:49 cf:50 cf:51 cf:52 cf:53 cf:54 cf:55 cf:56 cf:57 cf:58 cf:59 cf:60 cf:61 cf:62 cf:63 cf:64 cf:65 cf:66 cf:67 cf:68 cf:69 cf:70 cf:71 cf:72 cf:73 cf:74 cf:75 cf:76 cf:77 cf:78 cf:79 cf:80 cf:81 cf:82 cf:83 cf:84 cf:85 cf:86 cf:87 cf:88 cf:89 cf:90');