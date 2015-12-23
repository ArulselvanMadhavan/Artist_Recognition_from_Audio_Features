# cs6240-f15-project
CS6240 repository for the 'Parallel Data Processing in MapReduce' project

Load DataSet
./bin/spark-submit --master local[8] ~/cs6240-f15-project/DataCleanup/parsingTasks/loadData_PySpark.py ~/sample.txt ~/cs6240-f15-project/DataCleanup/parsingTasks/libraries/

Create Test and train

New:
./bin/spark-submit --class com.cs6240.msd.GenerateLibSVM --master local[8] --driver-memory 4G ~/jars/spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar $HBASE_HOME/conf/hbase-site.xml timbre_full 20 /mnt/snap/models/20/trainExtra.txt /mnt/snap/models/20/testExtra.txt /mnt/snap/models/20/trainSVM.txt /mnt/snap/models/20/testSVM.txt

Old:
./bin/spark-submit --class com.cs6240.msd.test --master local[2] --driver-memory 4G ~/cs6240/songs_reader/target/spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar /usr/local/hbase/hbase-0.98.15-hadoop2/conf/hbase-site.xml ~/cs6240/songs_reader/train.txt ~/cs6240/songs_reader/test.txt sample_timbre 50

For Prediction

1. Generate the Model:

./bin/spark-submit --class com.cs6240.msd.PredictFromSVM --master local[8] --driver-memory 4G ~/cs6240/songs_reader/target/spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar /home/arul/cs6240/songs_reader/trainSVM.txt /home/arul/cs6240/songs_reader/testSVM.txt 210 /home/arul/cs6240/songs_reader/

where 
210 is the (number of artists + 1). Find this count by running a tail command on the testSVM.txt
1. All other arguments are just filenames where you want to find traindata , testdata, numner of classes(see above)
and where to save the model. (This model will be used by the next program)

2. Predict

./bin/spark-submit --clas com.cs6240.msd.PredictFromModel --master local[8] --driver-memrory 4G ~/cs6240/songs_reader/target/spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar
/home/arul/models/50/model  /home/arul/models/50/testSVM.txt /home/arul/models/50/results.txt

Args: 
1. Path to the model
2. testSVM.txt
3. Path to save the results

Old:(Not used)
 ./bin/spark-submit --class com.cs6240.msd.trainAndTest --master local[8] --driver-memory 4G ~/cs6240/songs_reader/target/spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar ~/cs6240/songs_reader/sample_train.txt ~/cs6240/songs_reader/sample_test.txt sample_timbre 599 /usr/local/hbase/Home/hbase-site.xml ~/cs6240/songs_reader/results.txt ~/cs6240/songs_reader/model.txt


Things to do as soon as you start the Hbase machine


1. Start the ssh agent
eval `ssh-agent -s`
2. Add your ssh key
ssh-add yourpem.pem
3. Test ssh connection 
ssh localhost - success?
4. mount the volume

4a) sudo file -s /dev/xvdg

/dev/xvdg: Linux rev 1.0 ext3 filesystem data, UUID=a218826b-dcfd-444a-b553-7b762f4b0116 (large files)

4b)sudo mount -t ext4 /dev/xvdg /mnt/snap/

DO NOT FORMAT the hdfs FS.

DO THIS ONLY FOR THE FIRST TIME: Copy the bashrc in this repository into your ec2 user's bashrc
5 start-dfs.sh
6.start-yarn.sh
7.start-hbase.sh
# Artist_Recognition_from_Audio_Features
