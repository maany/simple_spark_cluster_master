#!/bin/bash

echo "Copy Hadoop conf files to /root/hadoop/etc/hadoop/"
cp /etc/simple_grid/config/hadoop_env.sh /root/hadoop/etc/hadoop/
cp /etc/simple_grid/config/core-site.xml /root/hadoop/etc/hadoop/
cp /etc/simple_grid/config/hdfs-site.xml /root/hadoop/etc/hadoop/
cp /etc/simple_grid/config/mapred-site.xml /root/hadoop/etc/hadoop/
cp -f /etc/simple_grid/config/yarn-site.xml /root/hadoop/etc/hadoop/
cp -f /etc/simple_grid/config/slaves /root/hadoop/etc/hadoop/

echo "Copying spark config files"
cp /etc/simple_grid/config/spark-defaults.conf /root/hadoop/spark/conf/

cat /etc/simple_grid/config/hadoop_env.sh >> ~/.bashrc
cat /etc/simple_grid/config/spark_hadoop_env.sh >> ~/.bashrc
source ~/.bashrc

echo "Formatting HDFS"
hdfs namenode -format

echo "Adding known_hosts"

file="/etc/simple_grid/config/slaves_ip"
for var in $(cat $file)
do
ssh-keyscan -H $var >> ~/.ssh/known_hosts
echo " $var"
done

echo "Starting HDFS"
start-dfs.sh

echo "Start YARN"
start-yarn.sh

echo "HDFS report"
hdfs dfsadmin -report

echo "YARN nodes"
yarn node -list

echo "Initializing the Spark Hadoop Master Container"

echo "Creating spark-logs hdfs directory"
hdfs dfs -mkdir /spark-logs

echo "Start Spark History Server"
$SPARK_HOME/sbin/start-history-server.sh

echo "All Done!"