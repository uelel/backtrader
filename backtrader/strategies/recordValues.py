import backtrader as bt
import datetime

class RecordValues(bt.Strategy):
    """Record values
       Values are created in subclass
       Output log complies with csv format"""
    
    params = dict(fileName='',
                  columnNames=list())
    
    def __init__(self):
        self.cnt = 0
        
        # Write data info into file
        with open(self.p.fileName, 'w') as file:
            file.write('# Execution time: %s\n' % (datetime.datetime.utcnow().strftime('%d.%m.%Y %H:%M UTC')))
        
        # Write column names into file
        with open(self.p.fileName, 'a') as file:
            file.write(','.join(self.params.columnNames)+'\n')
        
    def log(self,
            txt,
            dt=None):
        
        # Write log to file
        dt = bt.num2date(dt) or bt.num2date(self.data.datetime[0])
        with open(self.p.fileName, 'a') as file:
            file.write('%s,%s\n' % (dt.strftime("%d.%m.%Y %H:%M"), txt))
    
    def prenext(self):
        self.cnt += 1
    
    def nextstart(self):
        self.cnt += 1
        
        # Print progress to stdout
        print('Backtesting: %.2f%%' % (self.cnt*100/self.datas[0].p.len), end='\r')
    
    def next(self):
        self.cnt += 1
        
        # Print progress to stdout
        print('Backtesting: %.2f%%' % (self.cnt*100/self.datas[0].p.len), end='\r')
    
    def stop(self):
        
        # Print final progress to stdout
        print('Backtesting: %.2f%%' % (self.cnt*100/self.datas[0].p.len))
