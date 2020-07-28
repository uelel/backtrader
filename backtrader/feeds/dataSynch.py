import pandas as pd
import numpy as np

class DataSynchronizer():
    """Instantiates provided data feeds
       Data feeds are provided with addData method
       Slices provided data feeds so that each data feed containes
        only candles with same datetimes (i.e. datas are synchronized)
       Adds synchronized data feeds into provided cerebro"""

    def __init__(self):

        self.instDct = dict()
        self.dtLst = list()

    def addData(self, dct):
        """Instantiate provided data feed with provided arguments
           Save instance reference to instDct"""
        self.instDct[dct['name']] = dct['method'](**dct['kwargs'])

    def synchronize(self):
        """Actual synchronization is implemented here"""

        # load data into full attribute of each data feed object
        for i, (key, inst) in enumerate(self.instDct.items()):
            print('DataSynchronizer: initiating datafeed "%s"' % key)
            inst.init()
            inst.p.preloaded = True

        # synchronize Pandas dfs
        if all([type(inst.full) == pd.core.frame.DataFrame
                for inst
                in self.instDct.values()]):
           
            # find dts that are present in all data feeds
            for i, row in enumerate(list(self.instDct.values())[0].full.iterrows()):
                print('DataSynchronizer: looking for overlapping dts %.1f%%' % \
                      (i*100/list(self.instDct.values())[0].full.shape[0]),
                      end='\r')
                
                dt = np.datetime64(row[1]['time'])
                if all([dt in inst.full.time.values
                        for inst
                        in self.instDct.values()]):
                    self.dtLst.append(dt)
           
            print('DataSynchronizer: looking for overlapping dts 100.0%')

            # crop data feeds based on datetimes in dtLst
            print('DataSynchronizer: cropping data feeds based on overlapping dts')
            for inst in self.instDct.values():
                inst.full = inst.full[inst.full['time'].isin(self.dtLst)]

    def process(self, cerebro):
        """Execute synchronization and add synchronized
           data feeds into cerebro"""

        self.synchronize()

        print('DataSynchronizer: adding synchronized data feeds to cerebro')
        for name, inst in self.instDct.items():
            cerebro.adddata(inst, name=name)
