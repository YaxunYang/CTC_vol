
# coding: utf-8

# # Option Volume Data Processing

import pandas as pd

class OptionVolume(object):
    def __init__(self, workbook, Ticker, erase_samevol=True):
        self.wb = workbook # pandas.io.excel.ExcelFile
        self.Ticker = Ticker # underlying Ticker
        self._parse_spreadsheet_()
        self.OptTickers = self.df.columns
        self.expdates_existed = list(set([x.split()[2] for x in self.OptTickers]))
        if erase_samevol: self._erase_samevol_()
        
    def _parse_spreadsheet_(self):        
        sheetname = [x for x in self.wb.sheet_names if self.Ticker==x.split()[0]]
        # raise error if underlying Ticker not found in sheet names
        if len(sheetname)==0: raise ValueError('%s not found in sheets, parsing spreadsheet failed' %ticker)

        self.df = self.wb.parse(sheetname[0],header=1,index_col='Dates',skiprows=4)

    def _erase_samevol_(self):
        self.df[self.df.eq(self.df.shift())] = None
        
    def _identify_cols_(self, expdate, call_put):
#         strcmd = ['True']
#         if expdate:
#             strcmd.append('x.split()[2] in expdate')
#             self.cols = [x for x in self.OptTickers if expdate==x.split()[2]]
            
        if call_put:
            t = call_put.upper()[0]
#             strcmd.append('t==x.split()[3][0]')
            self.cols = [x for x in self.OptTickers if expdate==x.split()[2] and t==x.split()[3][0]]
        else:
            self.cols = [x for x in self.OptTickers if expdate==x.split()[2]]
            
#         self.cols = eval('[x for x in self.OptTickers if '+' and '.join(strcmd)+']')
        if len(self.cols)==0: raise ValueError('No %s data expired at %s was found, formatting option volume failed' %(call_put or '',expdate))

    def _aggregate_vol_(self, expdates, call_put):
        self.OptVol = pd.DataFrame(index=self.df.index) # initialization
        
        # set to all existed expiration dates if user not specify
        expdates = expdates or self.expdates_existed if type(expdates)!=str else [expdates]
        
        # add column and aggregate vol for each expiration date and option type
        for dt in expdates:
            self._identify_cols_(dt, call_put) # identify target cols relevant to expdate and call put type
            OptVol_colname = [self.Ticker,dt,call_put] if call_put else [self.Ticker,dt]
            self.OptVol['_'.join(OptVol_colname)] = self.df[self.cols].sum(axis=1)
            
    def get_volume(self, expdates=None, call_put=None):
        self._aggregate_vol_(expdates, call_put)
        
        return self.OptVol
    