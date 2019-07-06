# -*- coding: utf-8 -*-
import os


def getWarehouseDbConfigDict():
    source_db_name = os.getenv('MIS_WAREHOUSE_DB_NAME', 'db_name')
    source_db_username = os.getenv('MIS_WAREHOUSE_DB_USERNAME', 'username')
    source_db_password = os.getenv('MIS_WAREHOUSE_DB_PASSWORD', 'password')
    source_db_host = os.getenv('MIS_WAREHOUSE_DB_HOST', 'hostip')
    source_db_port = os.getenv('REPORTING_DB_PORT', 'source_db_port')

    db_conn_dict = dict(
        source_db_name=source_db_name,
        source_db_username=source_db_username,
        source_db_password=source_db_password,
        source_db_host=source_db_host,
        source_db_port=source_db_port
    )
    return db_conn_dict
