import configparser

def parse_db_config(filepath: str) -> dict[str,str]:
    
    config = configparser.ConfigParser()

    config.read(filepath)

    if config['Env']['environment'] == 'dev':
        db_config = config['Database']

    return db_config