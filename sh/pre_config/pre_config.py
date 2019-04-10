import argparse
import yaml
import dicttoxml
from xml.dom.minidom import parseString
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
    return parseString(dicttoxml.dicttoxml(core_site_content_array, custom_root='configuration',attr_type=False, item_func=lambda x: None).replace('<None>', '').replace('</None>', '')).toprettyxml()
def get_spark_hadoop_default_config_file_content(data, execution_id):
    current_lightweight_component = get_current_lightweight_component(data, execution_id)
    config = []
    config_section = current_lightweight_component['config']
    spark_event_log_enabled = config_section['spark_event_log_enabled']
    config.append("spark_event_log_enabled {value}".format(value=str(spark_event_log_enabled).lower()))
    spark_event_log_dir = config_section['spark_event_log_dir']
    config.append("spark_event_log_dir {value}".format(value=str(spark_event_log_dir).lower()))
    spark_driver_memory = config_section['spark_driver_memory']
    config.append("spark_driver_memory {value}".format(value=str(spark_driver_memory).lower()))
    spark_executor_memory = config_section['spark_executor_memory']
    config.append("spark_executor_memory {value}".format(value=str(spark_executor_memory).lower()))
    spark_yarn_am_memory = config_section['spark_yarn_am_memory']
    config.append("spark_yarn_am_memory {value}".format(value=str(spark_yarn_am_memory).lower()))
    spark_history_fs_log_directory = config_section['spark_history_fs_log_directory']
    config.append("spark_history_fs_log_directory {value}".format(value=str(spark_history_fs_log_directory).lower()))
    spark_history_fs_update_interval = config_section['spark_history_fs_update_interval']
    config.append("spark_history_fs_update_interval {value}".format(value=str(spark_history_fs_update_interval).lower()))
    # process supplemental config
    #supplemental_config = current_lightweight_component['supplemental_config']
    #for config_file in supplemental_config:
            #if config_file == "spark-defaults.conf":
                #for config_value in supplemental_config['spark-defaults.conf']:
                   # config.append(config_value)
    return "\n".join(config)


if __name__ == "__main__":
    args = parse_args()
    execution_id = args['execution_id']
    site_config_filename =  args['augmented_site_level_config_file']
    site_config = open(site_config_filename, 'r')
    data = yaml.load(site_config)
    output_dir = args['output_dir']
    spark_default_config_file = open("{output_dir}/spark-hadoop-defaults.conf".format(output_dir=output_dir), 'w')
    spark_default_config_file.write(get_spark_hadoop_default_config_file_content(data, execution_id))
    spark_default_config_file.close()

    core_site_xml = open("{output_dir}/core-site.xml".format(output_dir=output_dir), 'w')
    core_site_xml.write(get_core_site_xml_content(data,execution_id))
    core_site_xml.close()
    # spark_env_file = open("{output_dir}/spark_env.conf".format(output_dir=output_dir), 'w')
    # spark_env_file.write(get_spark_env_file_content(data, execution_id))
    # spark_env_file.close()