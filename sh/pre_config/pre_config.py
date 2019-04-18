import argparse
import yaml
import dicttoxml
from xml.dom.minidom import parseString
#import xml.etree.ElementTree as ET

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--site_config', help="Compiled Site Level Configuration YAML file")
    parser.add_argument('--execution_id', help="ID of lightweight component")
    parser.add_argument('--output_dir', help="Output directory")
    args = parser.parse_args()
    return {
        'augmented_site_level_config_file': args.site_config,
        'execution_id': args.execution_id,
        'output_dir': args.output_dir
    }


def get_current_lightweight_component(data, execution_id):
    current_lightweight_component = None
    for lightweight_component in data['lightweight_components']:
        if lightweight_component['execution_id'] == int(execution_id):
            current_lightweight_component = lightweight_component
            break
    return current_lightweight_component

def get_core_site_xml_content(data, execution_id):
    current_lightweight_component = get_current_lightweight_component(data,execution_id)
    core_site_content_array = []
    #core_site_content_array['configuration'] = []
    #configuration = core_site_content_array['configuration']
    ## fs.default.name
    fs_default_name = {}
    fs_default_name['property'] = []
    fs_default_name_property = fs_default_name['property']
    fs_default_name_property.append({'name': "fs.default.name"})
    fqdn = current_lightweight_component['deploy']['node']
    fs_default_name_property.append({'value': "hdfs://" +fqdn + ":9000"})
    core_site_content_array.append(fs_default_name)
    #configuration.append(fs_default_name)
    #xml.etree.ElementTree.tostring(core_site_content_array, encoding="UTF-8")
    xml_string =parseString(dicttoxml.dicttoxml(core_site_content_array, custom_root='configuration',attr_type=False, item_func=lambda x: None).replace('<None>', '').replace('</None>', '')).toprettyxml()
    xml_cropped_array = xml_string.split('\n')[1:]
    xml_cropped_array.insert(0, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
    xml_cropped_array.insert(1, "<?xml-stylesheet type=\"text/xml\" href=\"configuration.xml\"?>")
    return '\n'.join(xml_cropped_array)

def get_hdfs_site_xml_content(data, execution_id):
    current_lightweight_component = get_current_lightweight_component(data,execution_id)
    hdfs_site_content_array = []
    #core_site_content_array['configuration'] = []
    #configuration = core_site_content_array['configuration']
    ## dfs.namenode.name.dir
    dfs_namenode_name_dir = {}
    dfs_namenode_name_dir['property'] = []
    dfs_namenode_name_dir_property = dfs_namenode_name_dir['property']
    dfs_namenode_name_dir_property.append({'name': "dfs.namenode.name.dir"})
    dfs_namenode_name_dir_property.append({'value': "/root/data/nameNode"})
    hdfs_site_content_array.append(dfs_namenode_name_dir)
    ## dfs.datanode.data.dir
    dfs_datanode_data_dir = {}
    dfs_datanode_data_dir['property'] = []
    dfs_datanode_data_dir_property = dfs_datanode_data_dir['property']
    dfs_datanode_data_dir_property.append({'name': "dfs.datanode.data.dir"})
    dfs_datanode_data_dir_property.append({'value': "/root/data/dataNode"})
    hdfs_site_content_array.append(dfs_datanode_data_dir)
    ## dfs.replication
    dfs_replication = {}
    dfs_replication['property'] = []
    dfs_replication_property = dfs_replication['property']
    dfs_replication_property.append({'name': "dfs.replication"})
    fqdn = current_lightweight_component['config']['hdfs_dfs_replication']
    dfs_replication_property.append({'value': fqdn})
    hdfs_site_content_array.append(dfs_replication)
    #configuration.append(fs_default_name)
    xml_string = parseString(dicttoxml.dicttoxml(hdfs_site_content_array, custom_root='configuration',attr_type=False, item_func=lambda x: None).replace('<None>', '').replace('</None>', '')).toprettyxml()
    xml_cropped_array = xml_string.split('\n')[1:]
    xml_cropped_array.insert(0, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
    xml_cropped_array.insert(1, "<?xml-stylesheet type=\"text/xml\" href=\"configuration.xml\"?>")
    return '\n'.join(xml_cropped_array)

def get_mapred_site_xml_content(data, execution_id):
    current_lightweight_component = get_current_lightweight_component(data,execution_id)
    mapred_site_content_array = []
    ## mapreduce.framework.name
    mapreduce_framework_name = {}
    mapreduce_framework_name['property'] = []
    mapreduce_framework_name_property = mapreduce_framework_name['property']
    mapreduce_framework_name_property.append({'name': "mapreduce.framework.name"})
    mapreduce_framework_name_property.append({'value': "yarn"})
    mapred_site_content_array.append(mapreduce_framework_name)
    ## yarn.app.mapreduce.am.resource.mb
    yarn_app_mapreduce_am_resource_mb = {}
    yarn_app_mapreduce_am_resource_mb['property'] = []
    yarn_app_mapreduce_am_resource_mb_property = yarn_app_mapreduce_am_resource_mb['property']
    yarn_app_mapreduce_am_resource_mb_property.append({'name': "yarn.app.mapreduce.am.resource.mb"})
    fqdn = current_lightweight_component['config']['yarn_app_mapreduce_am_resource_mb']
    yarn_app_mapreduce_am_resource_mb_property.append({'value': fqdn})
    mapred_site_content_array.append(yarn_app_mapreduce_am_resource_mb)
    ## mapreduce.map.memory.mb
    mapreduce_map_memory_mb = {}
    mapreduce_map_memory_mb['property'] = []
    mapreduce_map_memory_mb_property = mapreduce_map_memory_mb['property']
    mapreduce_map_memory_mb_property.append({'name': "mapreduce.map.memory.mb"})
    fqdn = current_lightweight_component['config']['mapreduce_map_memory_mb']
    mapreduce_map_memory_mb_property.append({'value': fqdn})
    mapred_site_content_array.append(mapreduce_map_memory_mb)
    ## mapreduce.reduce.memory.mb
    mapreduce_reduce_memory_mb = {}
    mapreduce_reduce_memory_mb['property'] = []
    mapreduce_reduce_memory_mb_property = mapreduce_reduce_memory_mb['property']
    mapreduce_reduce_memory_mb_property.append({'name': "mapreduce.reduce.memory.mb"})
    fqdn = current_lightweight_component['config']['mapreduce_reduce_memory_mb']
    mapreduce_reduce_memory_mb_property.append({'value': fqdn})
    mapred_site_content_array.append(mapreduce_reduce_memory_mb)
    #configuration.append(fs_default_name)
    xml_string = parseString(dicttoxml.dicttoxml(mapred_site_content_array, custom_root='configuration',attr_type=False, item_func=lambda x: None).replace('<None>', '').replace('</None>', '')).toprettyxml()
    xml_cropped_array = xml_string.split('\n')[1:]
    xml_cropped_array.insert(0, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
    xml_cropped_array.insert(1, "<?xml-stylesheet type=\"text/xml\" href=\"configuration.xml\"?>")
    return '\n'.join(xml_cropped_array)

def get_yarn_site_xml_content(data, execution_id):
    yarn_site_content_array = []
    ## yarn.acl.enable
    current_lightweight_component = get_current_lightweight_component(data, execution_id)
    yarn_acl_enable = {}
    yarn_acl_enable['property'] = []
    yarn_acl_enable_property = yarn_acl_enable['property']
    yarn_acl_enable_property.append({'name': "yarn.acl.enable"})
    yarn_acl_enable_property.append({'value': '0'})
    yarn_site_content_array.append(yarn_acl_enable)
    ## yarn.resourcemanager.hostname
    yarn_resourcemanager_hostname = {}
    yarn_resourcemanager_hostname['property'] = []
    yarn_resourcemanager_hostname_property = yarn_resourcemanager_hostname['property']
    yarn_resourcemanager_hostname_property.append({'name': 'yarn.resourcemanager.hostname'})
    fqdn = current_lightweight_component['deploy']['node']
    yarn_resourcemanager_hostname_property.append({'value': fqdn})
    yarn_site_content_array.append(yarn_resourcemanager_hostname)
    ## yarn.nodemanager.aux-services
    yarn_nodemanager_aux_services = {}
    yarn_nodemanager_aux_services['property'] = []
    yarn_nodemanager_aux_services_property = yarn_nodemanager_aux_services['property']
    yarn_nodemanager_aux_services_property.append({'name': "yarn.nodemanager.aux-services"})
    yarn_nodemanager_aux_services_property.append({'value': "mapreduce_shuffle"})
    yarn_site_content_array.append(yarn_nodemanager_aux_services)
    ## yarn.nodemanager.resource.memory-mb
    yarn_nodemanager_resource_memory_mb = {}
    yarn_nodemanager_resource_memory_mb['property'] = []
    yarn_nodemanager_resource_memory_mb_property = yarn_nodemanager_resource_memory_mb['property']
    yarn_nodemanager_resource_memory_mb_property.append({'name': "yarn.nodemanager.resource.memory-mb"})
    fqdn = current_lightweight_component['config']['yarn_nodemanager_resource_memory_mb']
    yarn_nodemanager_resource_memory_mb_property.append({'value': fqdn})
    yarn_site_content_array.append(yarn_nodemanager_resource_memory_mb)
    ## yarn.scheduler.maximum-allocation-mb
    yarn_scheduler_maximum_allocation_mb = {}
    yarn_scheduler_maximum_allocation_mb['property'] = []
    yarn_scheduler_maximum_allocation_mb_property = yarn_scheduler_maximum_allocation_mb['property']
    yarn_scheduler_maximum_allocation_mb_property.append({'name': "yarn.scheduler.maximum-allocation-mb"})
    fqdn = current_lightweight_component['config']['yarn_scheduler_maximum_allocation_mb']
    yarn_scheduler_maximum_allocation_mb_property.append({'value': fqdn})
    yarn_site_content_array.append(yarn_scheduler_maximum_allocation_mb)
    ## yarn.scheduler.minimum-allocation-mb
    yarn_scheduler_minimum_allocation_mb = {}
    yarn_scheduler_minimum_allocation_mb['property'] = []
    yarn_scheduler_minimum_allocation_mb_property = yarn_scheduler_minimum_allocation_mb['property']
    yarn_scheduler_minimum_allocation_mb_property.append({'name': "yarn.scheduler.minimum-allocation-mb"})
    fqdn = current_lightweight_component['config']['yarn_scheduler_minimum_allocation_mb']
    yarn_scheduler_minimum_allocation_mb_property.append({'value': fqdn})
    yarn_site_content_array.append(yarn_scheduler_minimum_allocation_mb)
    ## yarn.nodemanager.vmem-check-enabled
    yarn_nodemanager_vmem_check_enabled = {}
    yarn_nodemanager_vmem_check_enabled['property'] = []
    yarn_nodemanager_vmem_check_enabled_property = yarn_nodemanager_vmem_check_enabled['property']
    yarn_nodemanager_vmem_check_enabled_property.append({'name': "yarn.nodemanager.vmem-check-enabled"})
    yarn_nodemanager_vmem_check_enabled_property.append({'value': "false"})
    yarn_site_content_array.append(yarn_nodemanager_vmem_check_enabled)
    xml_string = parseString(dicttoxml.dicttoxml(yarn_site_content_array, custom_root='configuration',attr_type=False, item_func=lambda x: None).replace('<None>', '').replace('</None>', '')).toprettyxml()
    xml_cropped_array = xml_string.split('\n')[1:]
    xml_cropped_array.insert(0, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
    xml_cropped_array.insert(1, "<?xml-stylesheet type=\"text/xml\" href=\"configuration.xml\"?>")
    return '\n'.join(xml_cropped_array)

def get_spark_hadoop_default_config_file_content(data, execution_id):
    current_lightweight_component = get_current_lightweight_component(data, execution_id)
    config = []
    config_section = current_lightweight_component['config']
    config.append("spark.master {value}".format(value=str("yarn").lower()))
    spark_event_log_enabled = config_section['spark_event_log_enabled']
    config.append("spark.event.log.enabled {value}".format(value=str(spark_event_log_enabled).lower()))
    spark_event_log_dir = config_section['spark_event_log_dir']
    config.append("spark.event.log.dir {value}".format(value=str(spark_event_log_dir).lower()))
    spark_driver_memory = config_section['spark_driver_memory']
    config.append("spark.driver.memory {value}".format(value=str(spark_driver_memory).lower()))
    spark_yarn_am_memory = config_section['spark_yarn_am_memory']
    config.append("spark.yarn.am.memory {value}".format(value=str(spark_yarn_am_memory).lower()))
    spark_executor_memory = config_section['spark_executor_memory']
    config.append("spark.executor.memory {value}".format(value=str(spark_executor_memory).lower()))
    config.append("spark.history.provider {value}".format(value=str("org.apache.spark.deploy.history.FsHistoryProvider")))
    spark_history_fs_log_directory = config_section['spark_history_fs_log_directory']
    config.append("spark.history.fs.logDirectory {value}".format(value=str(spark_history_fs_log_directory).lower()))
    spark_history_fs_update_interval = config_section['spark_history_fs_update_interval']
    config.append("spark.history.fs.update.interval {value}".format(value=str(spark_history_fs_update_interval).lower()))
    config.append("spark.history.ui.port {value}".format(value=str("18080").lower()))
    # process supplemental config
    #supplemental_config = current_lightweight_component['supplemental_config']
    #for config_file in supplemental_config:
            #if config_file == "spark-defaults.conf":
                #for config_value in supplemental_config['spark-defaults.conf']:
                   # config.append(config_value)
    return "\n".join(config)

def get_slave_file_content(data, execution_id):
    execution_ids = []
    slaves = []
    lightweight_components = data['lightweight_components']
    for lightweight_component in lightweight_components:
        if lightweight_component['type'] == "spark_hadoop_worker":
            execution_ids.append(lightweight_component['execution_id'])
    for execution_id in execution_ids:
        for dns_info in data['dns']:
            if dns_info['execution_id'] == execution_id:
                slaves.append(dns_info['container_fqdn'])
    return "\n".join(slaves)

def get_slave_ip_file_content(data, execution_id):
    execution_ids = []
    slaves = []
    lightweight_components = data['lightweight_components']
    for lightweight_component in lightweight_components:
        if lightweight_component['type'] == "spark_hadoop_worker":
            execution_ids.append(lightweight_component['execution_id'])
    for execution_id in execution_ids:
        for dns_info in data['dns']:
            if dns_info['execution_id'] == execution_id:
                slaves.append(dns_info['container_ip'])
    return "\n".join(slaves)

def get_spark_env_file_content():
    spark_env = []
    spark_env.append("export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.201.b09-2.el7_6.x86_64/jre")
    return "\n".join(spark_env)

def get_run_script_env_file_content(data, execution_id):
    slaves = []
    dns = None
    for dns_info in data['dns']:
        if dns_info['execution_id'] == int(execution_id):
            dns = dns_info
        else:
            slaves.append("{host}:{ip}".format(host=dns_info['container_fqdn'], ip=dns_info['container_ip']))
    env = ["export CONTAINER_FQDN={host}".format(host=dns['container_fqdn']),
           "export CONTAINER_IP={ip}".format(ip=dns['container_ip']),
           "export PORTS=('8080' '7077' '6066' '8088' '50070' '18080' '9000')",
           "export NODES=({nodes})".format(nodes=" ".join(slaves)),
           ]
    return '\n'.join(env)


if __name__ == "__main__":
    args = parse_args()
    execution_id = args['execution_id']
    site_config_filename =  args['augmented_site_level_config_file']
    site_config = open(site_config_filename, 'r')
    data = yaml.load(site_config, Loader=yaml.FullLoader)
    output_dir = args['output_dir']
    spark_default_config_file = open("{output_dir}/spark-defaults.conf".format(output_dir=output_dir), 'w')
    spark_default_config_file.write(get_spark_hadoop_default_config_file_content(data, execution_id))
    spark_default_config_file.close()

    slave_file = open("{output_dir}/slaves".format(output_dir=output_dir), 'w')
    slave_file.write(get_slave_file_content(data, execution_id))
    slave_file.close()

    slave_file = open("{output_dir}/slaves_ip".format(output_dir=output_dir), 'w')
    slave_file.write(get_slave_ip_file_content(data, execution_id))
    slave_file.close()

    core_site_xml = open("{output_dir}/core-site.xml".format(output_dir=output_dir), 'w')
    core_site_xml.write(get_core_site_xml_content(data,execution_id))
    core_site_xml.close()

    hdfs_site_xml = open("{output_dir}/hdfs-site.xml".format(output_dir=output_dir), 'w')
    hdfs_site_xml.write(get_hdfs_site_xml_content(data,execution_id))
    hdfs_site_xml.close()

    mapred_site_xml = open("{output_dir}/mapred-site.xml".format(output_dir=output_dir), 'w')
    mapred_site_xml.write(get_mapred_site_xml_content(data,execution_id))
    mapred_site_xml.close()

    yarn_site_xml = open("{output_dir}/yarn-site.xml".format(output_dir=output_dir), 'w')
    yarn_site_xml.write(get_yarn_site_xml_content(data,execution_id))
    yarn_site_xml.close()

    spark_env_file = open("{output_dir}/hadoop_env.sh".format(output_dir=output_dir), 'w')
    spark_env_file.write(get_spark_env_file_content())
    spark_env_file.close()

    run_script_env = open("{output_dir}/run_script.env".format(output_dir=output_dir), 'w')
    run_script_env.write(get_run_script_env_file_content(data, execution_id))
    run_script_env.close()