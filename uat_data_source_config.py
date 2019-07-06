# -*- coding: utf-8 -*-
import os


def getUATDataSourceConnString():
    source_db_username = os.getenv(
        'REPORTING_DB_USERNAME', 'source_db_username')
    source_db_password = os.getenv(
        'REPORTING_DB_PASSWORD', 'source_db_password')
    source_db_host = os.getenv('REPORTING_DB_HOST', 'source_db_host')
    source_db_port = os.getenv('REPORTING_DB_PORT', 'source_db_port')
    source_db_service = os.getenv(
        'REPORTING_DB_SERVICENAME', 'source_db_service')
    oracle_connection_string = '{source_db_username}/{source_db_password}@{source_db_host}:{source_db_port}/{source_db_service}'.format(
        source_db_username=source_db_username,
        source_db_password=source_db_password,
        source_db_host=source_db_host,
        source_db_port=source_db_port,
        source_db_service=source_db_service
    )
    return oracle_connection_string
