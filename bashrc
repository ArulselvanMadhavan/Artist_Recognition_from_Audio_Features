# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

# User specific aliases and functions
# -- HADOOP ENVIRONMENT VARIABLES START -- #
export JAVA_HOME=/usr/local/java/Home
export PATH=$JAVA_HOME/bin:$PATH
export HADOOP_HOME=/usr/local/hadoop/Home
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/
# -- HADOOP ENVIRONMENT VARIABLES END -- #
export HBASE_HOME=/usr/local/hbase/Home
export PHOENIX_HOME=/usr/local/phoenix/Home

export PATH=$HADOOP_HOME/bin:$HBASE_HOME/bin:$PHOENIX_HOME/bin:$PATH
export CLASSPATH=$PHOENIX_HOME/bin

#SPARK_HOME
export SPARK_HOME=/usr/local/spark/Home
export SCALA_HOME=$SPARK_HOME/build/scala-2.10.4
export PATH=$SPARK_HOME:$SCALA_HOME:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

#MAVEN
export M2_HOME=/usr/local/spark/Home/build/apache-maven-3.3.3
export M2=$M2_HOME/bin
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
export PATH=$M2:$PATH

# added by Anaconda 2.3.0 installer
export PATH="/home/ec2-user/anaconda/bin:$PATH"
