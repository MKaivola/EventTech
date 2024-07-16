import configparser

import boto3

def parse_db_config(filepath: str) -> dict[str,str]:
    """
    Parses database configuration from config file
    Gets sensitive prod configuration values from Parameter Store

    Arguments
    ---------
    filepath:
        A string representing the filepath to config file 
    """
    
    config = configparser.ConfigParser()

    config.read_file(open(filepath))

    db_config = dict(config['Database'])

    match config['Env']['environment']:
        case 'dev':
            pass
        case 'prod':
            # Get sensitive configuration values from Parameter Store
            session = boto3.Session()
            ssm_client = session.client('ssm')
            rds_client = session.client('rds')

            params = ssm_client.get_parameters(['/dev/prod/EventTech/db/region',
                                                '/dev/prod/EventTech/db/endpoint'])
            
            region = params['Parameters'][0]['Value']
            endpoint = params['Parameters'][1]['Value']

            db_auth_token = rds_client.generate_db_auth_token(DBHostname = endpoint,
                                                              Port = db_config['port'],
                                                              DBUsername = db_config['username'],
                                                              Region = region)
            
            db_config['password'] = db_auth_token
            db_config['host'] = endpoint
        case _:
            raise ValueError('Unknown environment')

    return db_config