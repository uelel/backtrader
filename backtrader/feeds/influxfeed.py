#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import backtrader as bt
import backtrader.feed as feed
from ..utils import date2num
import datetime
import numpy as np

TIMEFRAMES = dict(
    (
        (bt.TimeFrame.Seconds, 's'),
        (bt.TimeFrame.Minutes, 'm'),
        (bt.TimeFrame.Days, 'd'),
        (bt.TimeFrame.Weeks, 'w'),
        (bt.TimeFrame.Months, 'm'),
        (bt.TimeFrame.Years, 'y'),
    )
)


class InfluxDB(feed.DataBase):
    frompackages = (
        ('influxdb', [('InfluxDBClient', 'idbclient')]),
        ('influxdb.exceptions', 'InfluxDBClientError')
    )

    params = (
        ('host', '127.0.0.1'),
        ('port', '8086'),
        ('username', None),
        ('password', None),
        ('database', None),
        ('timeframe', bt.TimeFrame.Days),
        ('startdate', None),
        ('high', 'high_p'),
        ('low', 'low_p'),
        ('open', 'open_p'),
        ('close', 'close_p'),
        ('volume', 'volume'),
        ('ointerest', 'oi'),
    )

    def start(self):
        super(InfluxDB, self).start()
        try:
            self.ndb = idbclient(self.p.host, self.p.port, self.p.username,
                                 self.p.password, self.p.database)
        except InfluxDBClientError as err:
            print('Failed to establish connection to InfluxDB: %s' % err)

        tf = '{multiple}{timeframe}'.format(
            multiple=(self.p.compression if self.p.compression else 1),
            timeframe=TIMEFRAMES.get(self.p.timeframe, 'd'))

        if not self.p.startdate:
            st = '<= now()'
        else:
            st = '>= \'%s\'' % self.p.startdate

        # The query could already consider parameters like fromdate and todate
        # to have the database skip them and not the internal code
        qstr = ('SELECT mean("{open_f}") AS "open", mean("{high_f}") AS "high", '
                'mean("{low_f}") AS "low", mean("{close_f}") AS "close", '
                'mean("{vol_f}") AS "volume", mean("{oi_f}") AS "openinterest" '
                'FROM "{dataname}" '
                'WHERE time {begin} '
                'GROUP BY time({timeframe}) fill(none)').format(
                    open_f=self.p.open, high_f=self.p.high,
                    low_f=self.p.low, close_f=self.p.close,
                    vol_f=self.p.volume, oi_f=self.p.ointerest,
                    timeframe=tf, begin=st, dataname=self.p.dataname)

        try:
            dbars = list(self.ndb.query(qstr).get_points())
        except InfluxDBClientError as err:
            print('InfluxDB query failed: %s' % err)

        self.biter = iter(dbars)

    def _load(self):
        try:
            bar = next(self.biter)
        except StopIteration:
            return False

        self.l.datetime[0] = date2num(dt.datetime.strptime(bar['time'],
                                                           '%Y-%m-%dT%H:%M:%SZ'))

        self.l.open[0] = bar['open']
        self.l.high[0] = bar['high']
        self.l.low[0] = bar['low']
        self.l.close[0] = bar['close']
        self.l.volume[0] = bar['volume']

        return True


class InfluxData(feed.DataBase):
    
    frompackages = (('influxdb', 'DataFrameClient'),
                    ('influxdb.exceptions', 'InfluxDBClientError'))

    params = dict(dbName=None, # fileName
                  fromdate=None, # optional starttime for backtesting
                  todate=None, # optional stoptime for backtesting
                  timeframe=bt.TimeFrame.Minutes, # Timeframe for bt
                  compression=1, # Timeframe for bt
                  loadMissing=False,
                  len=None,
                  missing=None,
                  preloaded=False)
    
    # Add extra attribute to lines object
    lines = ('spread',)

    def __init__(self):
        
        # Define full attribute to be accessed by DataSynchronizer class
        self.full = None

        # Assign timedelta parameter for strategy purposes
        if self.p.timeframe == bt.TimeFrame.Minutes:
            self.p.timedelta = datetime.timedelta(minutes=self.p.compression)
        
        # Assign granularity parameter
        if self.p.timeframe == bt.TimeFrame.Minutes:
            if self.p.compression <= 30: self.p.gran = 'M'+str(self.p.compression)
            elif self.p.compression == 60: self.p.gran = 'H1'
    
    def init(self):

        # Connect client
        try:
            self.client = DataFrameClient(host='127.0.0.1',
                                          port=8086,
                                          database=self.p.dbName)
        except InfluxDBClientError as e:
            print('Failed to establish connection with db: %s' % e)
        
        # Load dataframe with rates
        if self.p.loadMissing:
            query = 'SELECT "time", "open", "high", "low", "close", "spread"' \
                    'FROM "rates" WHERE time >= \'%s\' AND time < \'%s\'' % \
                    (self.p.fromdate.strftime('%Y-%m-%dT%H:%M:%SZ'),
                     self.p.todate.strftime('%Y-%m-%dT%H:%M:%SZ'))
        else:
            query = 'SELECT "time", "open", "high", "low", "close", "spread"' \
                    'FROM "rates" WHERE time >= \'%s\' AND time < \'%s\'' \
                    'AND ("status" = \'C\' OR "status" = \'A\')' % \
                    (self.p.fromdate.strftime('%Y-%m-%dT%H:%M:%SZ'),
                     self.p.todate.strftime('%Y-%m-%dT%H:%M:%SZ'))
        try:        
            self.full = self.client.query(query)['rates']
        except InfluxDBClientError as e:
            print('Error during loading data from db: %s' % e)
    
    def start(self):

        if not self.p.preloaded: self.init()

        # Count array candles
        query = 'SELECT COUNT("close") FROM "rates" WHERE time >= \'%s\'' \
                'AND time < \'%s\' AND ("status" = \'C\' OR "status" = \'A\')' % \
                (self.p.fromdate.strftime('%Y-%m-%dT%H:%M:%SZ'),
                 self.p.todate.strftime('%Y-%m-%dT%H:%M:%SZ'))
        length = self.client.query(query)
        self.p.len = length['rates']['count'][0]

        # Count missing candles
        query = 'SELECT COUNT("close") FROM "rates" WHERE time >= \'%s\'' \
                'AND time < \'%s\' AND "status" = \'M\'' % \
                (self.p.fromdate.strftime('%Y-%m-%dT%H:%M:%SZ'),
                 self.p.todate.strftime('%Y-%m-%dT%H:%M:%SZ'))
        missing = self.client.query(query)
        self.p.missing = missing['rates']['count'][0] if 'rates' in missing else 0

        # Update fromdate, todate values if needed
        self.p.fromdate = self.full.first_valid_index().to_pydatetime()
        self.p.todate = self.full.last_valid_index().to_pydatetime()
        
        # Create dataframe iterator
        self.itr = self.full.iterrows()

    def stop(self):
        if self.full is not None: self.full = None

    def _load(self):
        
        # If no file, no reading
        if self.full is None: return False
        
        try:
            candle = next(self.itr)
        except StopIteration:
            return False
        
        # Put rates to lines attribute
        self.lines.datetime[0] = bt.date2num(candle[0].to_pydatetime())
        
        if candle[1]['open'] == 0.0:
            self.lines.open[0] = np.nan
            self.lines.high[0] = np.nan
            self.lines.low[0] = np.nan
            self.lines.close[0] = np.nan
        
        else:
            self.lines.open[0] = candle[1]['open']
            self.lines.high[0] = candle[1]['high']
            self.lines.low[0] = candle[1]['low']
            self.lines.close[0] = candle[1]['close']
        
        if 'spread' in self.full.columns:
            self.lines.spread[0] = candle[1]['spread']
        
        return True


class InfluxPreloaded(feed.DataBase):
    """Implementation of data loading from influxDB"""

    frompackages = (('influxdb', 'DataFrameClient'),
                    ('influxdb.exceptions', 'InfluxDBClientError'))

    params = dict(df=None,
                  timeframe=bt.TimeFrame.Minutes, # Timeframe for bt
                  compression=1, # Timeframe for bt
                  len=None)
    
    # Add extra attribute to lines object
    lines = ('spread',)
    
    def __init__(self):
        
        # Define full attribute to be accessed by DataSynchronizer class
        self.full = None

        # Assign timedelta parameter for strategy purposes
        if self.p.timeframe == bt.TimeFrame.Minutes:
            self.p.timedelta = datetime.timedelta(minutes=self.p.compression)
        
        # Assign granularity parameter
        if self.p.timeframe == bt.TimeFrame.Minutes:
            if self.p.compression <= 30: self.p.gran = 'M'+str(self.p.compression)
            elif self.p.compression == 60: self.p.gran = 'H1'

    def start(self):

        self.full = self.p.df
        
        # get data length
        self.p.len = self.full.shape[0]

        # create row iterator
        self.itr = self.full.iterrows()
    
    def stop(self):
        if self.full is not None: self.full = None

    def _load(self):
        
        # If no file, no reading
        if self.full is None: return False
        
        try:
            candle = next(self.itr)
        except StopIteration:
            return False
        
        # Put rates to lines attribute
        self.lines.datetime[0] = bt.date2num(candle[0])
        
        if candle[1]['open'] == 0.0:
            self.lines.open[0] = np.nan
            self.lines.high[0] = np.nan
            self.lines.low[0] = np.nan
            self.lines.close[0] = np.nan
        else:
            self.lines.open[0] = candle[1]['open']
            self.lines.high[0] = candle[1]['high']
            self.lines.low[0] = candle[1]['low']
            self.lines.close[0] = candle[1]['close']
        
        if 'spread' in self.full.columns:
            self.lines.spread[0] = candle[1]['spread']

        return True
