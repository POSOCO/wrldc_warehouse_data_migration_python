# -*- coding: utf-8 -*-

import datetime as dt
from raw_freq_migrator import migrateRawFreqData

from_dt = dt.datetime(2019, 7, 1)
to_dt = dt.datetime(2019, 7, 3)

migrateRawFreqData(from_dt, to_dt)