# -*- coding: utf-8 -*-

import datetime as dt
from raw_freq_migrator import migrateRawFreqDataWithFetchWindow

# configuration inputs for data migration
from_dt = dt.datetime(2019, 7, 6)
to_dt = dt.datetime(2019, 7, 11)
fetch_window_interval = dt.timedelta(days=1)

migrateRawFreqDataWithFetchWindow(from_dt, to_dt, fetch_window_interval)