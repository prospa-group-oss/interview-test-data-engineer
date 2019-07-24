import yaml
import os

config_path = os.path.dirname(os.path.abspath(__file__))
def load_config():
    config_file = os.path.join(config_path,"configuration.yaml")
    with open(config_file,"r") as config_load :
        cust_configuration = yaml.load(config_load)
    return cust_configuration

def dwh_load_config():
    config_file = os.path.join(config_path, "dwh_config.yaml")
    with open(config_file,"r") as config_load :
        cust_configuration = yaml.load(config_load)
    return cust_configuration

def report_config():
    config_file = os.path.join(config_path, "report_config.yaml")
    with open(config_file,"r") as config_load :
        cust_configuration = yaml.load(config_load)
    return cust_configuration

def ddl_config():
    config_file = os.path.join(config_path, "ddl.yaml")
    with open(config_file,"r") as config_load :
        cust_configuration = yaml.load(config_load)
    return cust_configuration

if __name__ == "__main__":
    print(load_config())


