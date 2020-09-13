from google.cloud import bigquery

GCP_PROJECT_NAME = 'fifaeng'


def get_last_version_id_from_main_version(main_version):
    bigquery_connection = bigquery.Client(project=GCP_PROJECT_NAME)
    query = bigquery_connection.query('''
                SELECT 
                    version_id
                FROM sofifa.versions WHERE version_name = "FIFA {version}" 
                ORDER BY version_id DESC
                LIMIT 1
            '''.format(
        version=main_version
    ))
    return query.result()


def get_urls_from_version(version):
    bigquery_connection = bigquery.Client(project='fifaeng')
    query = bigquery_connection.query('SELECT value FROM sofifa.urls_{version}'.format(version=version))
    return query.result()