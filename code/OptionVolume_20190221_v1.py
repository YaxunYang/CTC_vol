
# coding: utf-8

# # Option Volume Data Processing

import pandas as pd

class OptionVolume(object):
    def __init__(self, workbook, Ticker, erase_samevol=False,header=0,index_col=0,skiprows=0,transpose=False,delimiter='_'):
        self.wb = workbook # pandas.io.excel.ExcelFile
        self.Ticker = Ticker # underlying Ticker
        self._parse_spreadsheet_(header,index_col,skiprows,transpose)
        self.OptTickers = self.df.columns
        self.delimiter = delimiter
        self.expdates_existed = sorted(list(set([x.split(self.delimiter)[1] for x in self.OptTickers])))
        if erase_samevol: self._erase_samevol_()
        
    def _parse_spreadsheet_(self,header,index_col,skiprows,transpose):    
        sheetname = [x for x in self.wb.sheet_names if self.Ticker==x.split()[0]]
        # raise error if underlying Ticker not found in sheet names
        if len(sheetname)==0: raise ValueError('%s not found in sheets, parsing spreadsheet failed' %self.Ticker)

        self.df = self.wb.parse(sheetname[0],header=header,index_col=index_col,skiprows=skiprows)
        if transpose: self.df = self.df.transpose()
            
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
            self.cols = set([x for x in self.OptTickers if expdate==x.split(self.delimiter)[1] and t==x.split(self.delimiter)[2][0]])
        else:
            self.cols = set([x for x in self.OptTickers if expdate==x.split(self.delimiter)[1]])
            
#         self.cols = eval('[x for x in self.OptTickers if '+' and '.join(strcmd)+']')
        if len(self.cols)==0: raise ValueError('No %s data expired at %s was found, formatting option volume failed' %(call_put,expdate))

    def _aggregate_vol_(self, expdates, call_put):
        self.OptVol = pd.DataFrame(index=self.df.index) # initialization
        
        # set to all existed expiration dates if user not specify
        expdates = expdates or self.expdates_existed if type(expdates)!=str else [expdates]
        
        # set to call and put if user declare call_put == 'A'
        if type(call_put)!=str: call_put = call_put or [None]
        elif call_put[0] == 'A': call_put = ['Call','Put'] 
        else: call_put = [call_put]
        
        # add column and aggregate vol for each expiration date and option type
        for dt in expdates:
            for callput in call_put:
                self._identify_cols_(dt, callput) # identify target cols relevant to expdate and call put type
                OptVol_colname = [self.Ticker,dt,callput] if callput else [self.Ticker,dt]
                self.OptVol['_'.join(OptVol_colname)] = self.df[self.cols].sum(axis=1)

    def get_volume(self, expdates=None, call_put=None, export2excel=False, excelname=None):
        self._aggregate_vol_(expdates, call_put)
        
        if export2excel:
            writer = pd.ExcelWriter(excelname, engine='xlsxwriter',datetime_format='MM/DD/YYYY')
            self.OptVol.to_excel(writer,sheet_name=self.Ticker)
            writer.save()
            
        return self.OptVol
    