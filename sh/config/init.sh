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



echo "All Done!"
