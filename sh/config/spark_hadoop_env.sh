#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.201.b09-2.el7_6.x86_64/jre
export PATH=/root/hadoop/spark/bin:/root/hadoop/bin:/root/hadoop/sbin:/usr/sue/sbin:/usr/sue/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export HADOOP_CONF_DIR=/root/hadoop/etc/hadoop
export SPARK_HOME=/root/hadoop/spark
export LD_LIBRARY_PATH="/root/hadoop/lib/native:$LD_LIBRARY_PATH"
export PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.7-src.zip:${PYTHONPATH}"
export PYSPARK_PYTHONPATH_SET=1