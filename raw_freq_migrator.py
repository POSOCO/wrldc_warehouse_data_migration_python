# -*- coding: utf-8 -*-
'''
psycopg2 connection string - http://initd.org/psycopg/docs/module.html
python efficient bulk insertion - https://nelsonslog.wordpress.com/2015/04/27/inserting-lots-of-data-into-a-remote-postgres-efficiently/
on update conflict for upsert - https://stackoverflow.com/questions/34514457/bulk-insert-update-if-on-conflict-bulk-upsert-on-postgres
[(20180222, '09:57:20', 49.921), (20180222, '09:57:30', 49.9292), (20180222, '09:57:40', 49.9311), (20180222, '09:57:50', 49.9327), (20180222, '09:58:00', 49.9296)]
'''
import cx_Oracle
import datetime as dt
import psycopg2

from uat_data_source_config import getUATDataSourceConnString
from warehouse_db_config import getWarehouseDbConfigDict

def migrateRawFreqData(from_dt, to_dt):
    from_date_key = int(from_dt.strftime('%Y%m%d'))
    #from_time_key = from_dt.strftime('%H:%M:%S')
    to_date_key = int(to_dt.strftime('%Y%m%d'))
    #to_time_key = to_dt.strftime('%H:%M:%S')

    warehouseConfigDict = getWarehouseDbConfigDict()
    oracle_connection_string = getUATDataSourceConnString()
    con = cx_Oracle.connect(oracle_connection_string)
    cur = con.cursor()

    cur.prepare('SELECT DATE_KEY, TIME_KEY, FREQ_VAL FROM STG_SCADA_FREQUENCY_NLDC where DATE_KEY between :from_date_key and :to_date_key and ISDELETED = :isdeleted order by DATE_KEY, TIME_KEY')

    cur.execute(None, {
                'isdeleted': 0, 'from_date_key': from_date_key, 'to_date_key': to_date_key})
    res = cur.fetchall()
    # print(res)

    cur.close()

    conn = psycopg2.connect(host=warehouseConfigDict['source_db_host'], dbname=warehouseConfigDict['source_db_name'],
                            user=warehouseConfigDict['source_db_username'], password=warehouseConfigDict['source_db_password'])
    cur = conn.cursor()

    # we will commit in multiples of 100 rows
    rowIter = 0
    insIncr = 100
    while rowIter < len(res):
        # set iteration values
        iteratorEndVal = rowIter+insIncr
        if iteratorEndVal >= len(res):
            iteratorEndVal = len(res)

        # Create row tuples
        freqInsertionTuples = []
        for insRowIter in range(rowIter, iteratorEndVal):
            freqRow = res[insRowIter]

            freqInsertionTuple = (dt.datetime.strptime(str(
                freqRow[0])+' '+freqRow[1], '%Y%m%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'), freqRow[2])
            freqInsertionTuples.append(freqInsertionTuple)

        # prepare sql for insertion and execute
        dataText = ','.join(cur.mogrify('(%s,%s)', row).decode("utf-8") for row in freqInsertionTuples)
        
        cur.execute(
            'insert into public."RawFrequencies" ("DataTime", "Frequency") values '+dataText+' on conflict ("DataTime") do update set "Frequency" = excluded."Frequency"')
        conn.commit()

        rowIter = iteratorEndVal

    # close cursor and connection
    cur.close()
    conn.close()