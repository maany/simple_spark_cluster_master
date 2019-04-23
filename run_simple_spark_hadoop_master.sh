#!/bin/bash
#docker build -t simple_spark_cluster_master_sh_pre_config ./sh/pre_config/
#docker run -it -e "EXECUTION_ID=0" -v $(pwd)/:/component_repository simple_spark_cluster_master_sh_pre_config bash

### CLEANUP ####
docker stop ${CONTAINER_FQDN} && docker rm ${CONTAINER_FQDN}

### BOOT EVENT ###
sudo docker build -t simple_spark_hadoop_master sh/
sudo docker run -itd \
    --name simple_spark_hadoop_master \
    --privileged \
    -p 7077:7077 \
    -p 8080:8080 \
    -p 6066:6066 \
    -p 8088:8088 \
    -p 50070:50070 \
    -p 18080:18080 \
    -p 9000:9000 \
    -p 8888:8888 \
    --net spark_swarm \
    --ip 10.1.1.10 \
    --add-host "spark-hadoop-worker-localhost01-1.cern.ch:10.1.1.11" \
    --hostname $(hostname) \
    -v $(pwd)/sh/config:/etc/simple_grid/config \
    -v $(pwd)/augmented_site_level_config_file.yaml:/etc/simple_grid/augmented_site_level_config_file.yaml \
    simple_spark_hadoop_master

#### PRE INIT HOOKS #####

echo "Please proceed with the init event by hand. Merci!"
### INIT EVENT ######
#sudo docker exec -t simple_spark_hadoop_master /bin/bash -c '/etc/simple_grid/config/init.sh'

#### POST INIT HOOKS ######
sudo docker exec -it simple_spark_hadoop_master bash
