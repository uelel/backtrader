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

from . import MovingAverageBase, ExponentialSmoothing
import numpy as np

class ExponentialMovingAverage(MovingAverageBase):
    '''
    A Moving Average that smoothes data exponentially over time.

    It is a subclass of SmoothingMovingAverage.

      - self.smfactor -> 2 / (1 + period)
      - self.smfactor1 -> `1 - self.smfactor`

    Formula:
      - movav = prev * (1.0 - smoothfactor) + newdata * smoothfactor

    See also:
      - http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
    '''
    alias = ('EMA2', 'MovingAverageExponential',)
    lines = ('ema',)

    def __init__(self):
        # Before super to ensure mixins (right-hand side in subclassing)
        # can see the assignment operation and operate on the line
        self.lines[0] = es = ExponentialSmoothing(
            self.data,
            period=self.p.period,
            alpha=2.0 / (1.0 + self.p.period))

        self.alpha, self.alpha1 = es.alpha, es.alpha1

        super(ExponentialMovingAverage, self).__init__()


class EMA(MovingAverageBase):
    '''
    Exponential moving average implemented iteratively
    skipVals option added to skip initial NaN data values
    '''

    params = dict(period=None,
                  skipVals=None)

    lines = ('ema',)

    def __init__(self):

        if hasattr(self.data, 'close'):
            self.data = self.data.close
        else:
            self.data = self.data
        
        self.addminperiod(self.p.skipVals + self.p.period)
        
        self.c = 2 / (self.p.period + 1)


    def nextstart(self):
        self.l.ema[0] = np.mean(self.data.get(size=self.p.period)) / self.p.period

    def next(self):

        self.l.ema[0] = self.l.ema[-1] + self.c * (self.data[0] - self.l.ema[-1])
