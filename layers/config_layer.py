import json


def get_path_to_db_tbs():

    """ Return path to original database tables from 'config.json' """

    with open('config.json') as conf_file:
        conf_data = json.load(conf_file)
        return conf_data['path_to_db_tables']


def get_path_to_pp_tbs():

    """ Return path to preproccessed tables from 'config.json' """

    with open('config.json') as conf_file:
        conf_data = json.load(conf_file)
        return conf_data['path_to_preproccesed_tables']