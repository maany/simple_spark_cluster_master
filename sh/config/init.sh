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

echo "Setup ssh"
cp -f /etc/simple_grid/config/id_rsa /root/.ssh/
cp -f /etc/simple_grid/config/id_rsa.pub /root/.ssh/
cp -f /etc/simple_grid/config/id_rsa.pub /root/.ssh/authorized_keys
cp -f /etc/simple_grid/config/ssh_host_rsa_key /etc/ssh/
cp -f /etc/simple_grid/config/ssh_host_rsa_key.pub /etc/ssh/
chmod 600 ~/.ssh/id_rsa
chmod 640 /etc/ssh/ssh_host_rsa_key
cat /etc/simple_grid/config/hadoop_env.sh >> ~/.bashrc
cat /etc/simple_grid/config/spark_hadoop_env.sh >> ~/.bashrc
source ~/.bashrc

echo "Copy ssh host keys for workers. Should be in a for loop:"
file="/etc/simple_grid/config/slaves"
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


echo "All Done!"
