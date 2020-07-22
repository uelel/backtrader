import pandas as pd

class DataSynchronizer():
    """Instantiates provided data feeds
       Slices provided data feeds so that each data feed containes
       only candles with same datetimes (i.e. synchronize them)
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
        for key, inst in self.instDct.items():
            inst.init()
            inst.p.preloaded = True

        # synchronize Pandas dfs
        if all([type(inst.full) == pd.core.frame.DataFrame
                for inst
                in self.instDct.values()]):
           
            # find dts that are present in all data feeds
            for row in list(self.instDct.values())[0].full.iterrows():
                
                dt = row[1]['time']
                
                if all([dt in inst.full.time.to_list()
                        for inst
                        in self.instDct.values()]):
                    self.dtLst.append(dt)
           
            # crop data feeds based on datetimes in dtLst
            for inst in self.instDct.values():
                inst.full = inst.full[inst.full['time'].isin(self.dtLst)]

    def process(self, cerebro):
        """Execute synchronization and add synchronized
           data feeds into cerebro"""

        self.synchronize()

        for name, inst in self.instDct.items():
            cerebro.adddata(inst, name=name)
