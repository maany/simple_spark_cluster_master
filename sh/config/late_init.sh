#!/bin/bash
echo "Copy ssh host keys for workers. Should be in a for loop:"
file="/etc/simple_grid/config/slaves_ip"
for var in $(cat $file)
do
ssh-keyscan -t rsa $var >> /etc/ssh/ssh_known_hosts
echo " $var"
done

ssh-keyscan simple-lc01.cern.ch >> /etc/ssh/ssh_known_hosts
ssh-keyscan 0.0.0.0 >> /etc/ssh/ssh_known_hosts

echo "Formatting HDFS"
hdfs namenode -format

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
