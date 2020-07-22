from backtrader import Indicator
import numpy as np

class SinTransform(Indicator):
    """Implementation of sinus transform indicator"""
    
    lines = ('pred',)
    
    params = dict(pastBars=300, predBars=100, harmNo=1)
    
    # Parameters for plotting
    plotinfo = dict(
        # subplot=True/False; whether to plot indicator in separate window
        subplot = False,
        # plot=True/False; whether to plot indicator or not
        plot = True,
        # plotname
        plotname = 'SMA',
        # plotabove=True/False; plot indicator above/below data
        plotabove = False,
        # plotymargin=0.15; percentage of plotted y margin above and below plot (15%)
        plotymargin = 0.15,
        # plotyticks=[]; list of y ticks to plot
        plotyticks = [20,50,80],
        # plothlines=[]
        plothlines = [50])
    
    
    def calcFreq(self, priceArray, pv, pastBars):
        """Quinn-Fernandes algorithm to fit constants w,m,c,s"""
        
        z = np.zeros(pastBars)
        a = 0.0
        b = 2.0
        z[0] = priceArray[0] - pv[0];
        
        while(abs(a-b) > 0.0001):
            a = b
            z[1] = priceArray[1] - pv[1] + a*z[0]
            num=z[0]*z[1]
            den=z[0]*z[0]
            for i in range(2,pastBars):
                z[i] = priceArray[i] - pv[i] + a*z[i-1] - z[i-2]
                num += z[i-1]*(z[i]+z[i-2])
                den += z[i-1]*z[i-1]
            b = num/den
        
        w = np.arccos(b/2.0)
        
        Sc = 0.0
        Ss = 0.0
        Scc = 0.0
        Sss = 0.0
        Scs = 0.0
        Sx = 0.0
        Sxx = 0.0
        Sxc = 0.0
        Sxs = 0.0
        
        for i in range(0, pastBars):
            cos = np.cos(w*i)
            sin = np.sin(w*i)
            Sc += cos
            Ss += sin
            Scc += cos*cos
            Sss += sin*sin
            Scs += cos*sin
            Sx += priceArray[i] - pv[i]
            Sxx += (priceArray[i] - pv[i])**2
            Sxc += (priceArray[i] - pv[i])*cos
            Sxs += (priceArray[i] - pv[i])*sin
            
        Sc /= pastBars
        Ss /= pastBars
        Scc /= pastBars
        Sss /= pastBars
        Scs /= pastBars
        Sx /= pastBars
        Sxx /= pastBars
        Sxc /= pastBars
        Sxs /= pastBars
        
        if w == 0.0:
            m = Sx
            c = 0.0
            s = 0.0
        else:
            den = (Scs-Sc*Ss)**2 - (Scc-Sc*Sc)*(Sss-Ss*Ss)
            c = ((Sxs-Sx*Ss)*(Scs-Sc*Ss) - (Sxc-Sx*Sc)*(Sss-Ss*Ss))/den
            s = ((Sxc-Sx*Sc)*(Scs-Sc*Ss) - (Sxs-Sx*Ss)*(Scc-Sc*Sc))/den
            m = Sx-c*Sc-s*Ss
        
        return w, m, c, s
    
    def _plotlabel(self):
        # This method returns a list of labels that will be displayed
        # behind the name of the indicator on the plot

        # The period must always be there
        plabels = [self.params.period]

        # Put only the moving average if it's not the default one
        plabels += ['some label']

    def __init__(self):
        self.addminperiod(self.params.pastBars)
        self.cnt = 0
        
    def prenext(self):
        self.cnt += 1
        
    def next(self):
        self.cnt += 1
        # Get price array
        priceArrayRev = self.data.open.get(ago=0,size=self.p.pastBars)
        # Reverse price array (latest candles first)
        priceArray = np.empty(0, float)
        for i in range(self.p.pastBars):
            priceArray = np.append(priceArray, priceArrayRev[self.p.pastBars-1-i])
        
        pv = np.zeros(self.p.pastBars)
        fv = np.zeros(self.p.predBars)
        # Calculate mean value from price array and put it to lines
        averPrice = np.mean(priceArray)
        for i in range(self.p.pastBars):
            pv[i] = averPrice
        for i in range(self.p.predBars):
            fv[i] = averPrice
        
        # Calculate transform function
        for _ in range(self.p.harmNo):
            # Calculate function parameters
            w, m, c, s = self.calcFreq(priceArray, pv, self.p.pastBars)
            #if self.cnt == self.p.pastBars:
            #        print(w,m,c,s)
            # Create lines
            for i in range(self.p.pastBars):
                pv[i] += m + c*np.cos(w*i) + s*np.sin(w*i)
            for i in range(self.p.predBars):
                fv[i] += m + c*np.cos(w*i) - s*np.sin(w*i)
        
        # Write resulting curves to lines
        #for i in range(self.p.pastBars):
        #    pastTrans[self.p.pastBars-1-i] = pv[self.p.pastBars-1-i]
        for i in range(self.p.predBars):
            #print(self.p.predBars-1-i, fv[i], self.lines.predTrans[self.p.predBars-1-i])
            self.lines.pred[-(self.p.predBars-1-i)] = fv[i]
        
        #if self.cnt == self.p.pastBars:
        #    with open('/home/jan/forex/oanda-api/fftVal', 'w') as file:
        #        for i in range(self.p.pastBars):
        #            file.write(str(pv[self.p.pastBars-1-i])+'\n')
        #        for i in range(self.p.predBars):
        #            file.write(str(fv[i])+'\n')
