import yaml


def config():

    file_config = open('config/config.yaml', encoding='utf8')
    conf = yaml.load(file_config, Loader=yaml.FullLoader)
    return conf
