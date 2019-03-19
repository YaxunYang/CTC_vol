
# coding: utf-8

# # Option Volume Data Processing

import pandas as pd
import numpy as np
import re
from datetime import datetime,timedelta

class OptionVolume(object):
    def __init__(self, workbook, Ticker, erase_holiday=True, erase_samevol=False,header=0,index_col=0,skiprows=0,transpose=False,delimiter='_'):
        self.wb = workbook # pandas.io.excel.ExcelFile
        self.Ticker = Ticker # underlying Ticker
        self._parse_spreadsheet_(header,index_col,skiprows,transpose)
        self.OptTickers = self.df.columns
        self.delimiter = delimiter
        self.expdates_existed = sorted(list(set([x.split(self.delimiter)[1] for x in self.OptTickers])))
        self.expdate2WM = dict([(x,'M' if '14'<x[-2:]<'22' else 'W') for x in self.expdates_existed])

        if erase_samevol: self._erase_samevol_()
        if erase_holiday: self._erase_holiday_()#print_holidays=True)
        
    def _parse_spreadsheet_(self,header,index_col,skiprows,transpose):    
        sheetname = [x for x in self.wb.sheet_names if self.Ticker==x.split()[0]]
        # raise error if underlying Ticker not found in sheet names
        if len(sheetname)==0: raise ValueError('%s not found in sheets, parsing spreadsheet failed' %self.Ticker)

        self.df = self.wb.parse(sheetname[0],header=header,index_col=index_col,skiprows=skiprows)
        if transpose: self.df = self.df.transpose()
        
    def _erase_samevol_(self):
        self.df[self.df.eq(self.df.shift())] = None
        
    def _erase_holiday_(self,print_holidays=False):
        self.TotalVol = self.df.sum(axis=1)
        isholiday = self.TotalVol == 0 # define zero total volume date as holiday
        self.df = self.df[-isholiday]

        if print_holidays and isholiday.sum():
            # set print holiday setting
            print_len = 5 # maximum print length

            self.holidays = self.TotalVol.index[isholiday].astype(str).to_list()
            holidays_forpriting = [self.holidays[i:i+print_len] for i in range(0,len(self.holidays),print_len)]
            print('The following zero total volume date been removed: \n\t'+'\n\t'.join([' '.join(x) for x in holidays_forpriting]))
    
    def _identify_cols_(self, expdate, call_put):
#         strcmd = ['True']
#         if expdate:
#             strcmd.append('x.split()[2] in expdate')
#             cols = [x for x in self.OptTickers if expdate==x.split()[2]]
            
        if call_put:
            t = call_put.upper()[0]
#             strcmd.append('t==x.split()[3][0]')
            cols = list(set([x for x in self.OptTickers if expdate==x.split(self.delimiter)[1] and t==x.split(self.delimiter)[2][0]]))
        else:
            cols = list(set([x for x in self.OptTickers if expdate==x.split(self.delimiter)[1]]))
            
#         cols = eval('[x for x in self.OptTickers if '+' and '.join(strcmd)+']')
        if len(cols)==0: raise ValueError('No %s data expired at %s was found, formatting option volume failed' %(call_put,expdate))
        return cols
        
    def _aggregate_vol_(self, expdates, call_put, weeklys):
        self.OptVol = pd.DataFrame(index=self.df.index) # initialization
        
        # set to all existed expiration dates if user not specify
        expdates = expdates or self.expdates_existed if type(expdates)!=str else [expdates]
        
        # set call_put = ['Call','Put'] if user declare call_put == 'A'
        if type(call_put)!=str: call_put = call_put or [None]
        elif call_put[0] == 'A': call_put = ['Call','Put'] 
        else: call_put = [call_put]
        
        # set weeklys = ['W','M'] if user declare weeklys == 'A'
        if weeklys is not None and weeklys[0] == 'A': weeklys = ['W','M']
        
        # add column and aggregate vol for each expiration date and option type
        for expdate in expdates:
            if expdate not in self.expdate2WM: raise ValueError('Expiration date %s not found, get option volume failed' %expdate)
            weekly = self.expdate2WM[expdate]
            for callput in call_put:
                if weeklys is None or weekly in weeklys:
                    cols = self._identify_cols_(expdate, callput) # identify target cols relevant to expdate and call put type
                    OptVol_colname = [self.Ticker,expdate]+([callput] if callput else [])+([weekly] if weeklys else [])
                    self.OptVol['_'.join(OptVol_colname)] = self.df[cols].sum(axis=1)
    
    def _export2excel_(self, df, excelname):
        writer = pd.ExcelWriter(excelname, engine='xlsxwriter',datetime_format='MM/DD/YYYY')
        df.to_excel(writer,sheet_name=self.Ticker)
        writer.save()

    def get_volume(self, expdates=None, call_put=None, weeklys=None, export2excel=False, excelname=None):
        self._aggregate_vol_(expdates, call_put, weeklys)
        
        if export2excel: self._export2excel_(self.OptVol, excelname)

        return self.OptVol
        
    def _chop_OptTickers_(self, OptTickers):
        # chop away the OptTickers that does not expire yet
        lastdate = self.df.index[-1]
        newOptTickers = [x for x in OptTickers if datetime.strptime(x.split("_")[1],"%Y%m%d") <= lastdate]
        return newOptTickers
    
    def get_tau(self, OptVol, week_adj=0, export2excel=False, excelname=None):
        # derive the week number for OptVol.index
        Mondays = OptVol.index - pd.TimedeltaIndex(data=OptVol.index.weekday,unit='D')
        Week_N = (Mondays - Mondays[0]).days//7
        
        taus_df = pd.DataFrame(index=OptVol.index,columns=OptVol.columns)
        for OptTicker in self._chop_OptTickers_(OptVol.columns):
            expdate = OptTicker.split("_")[1]
            date_iloc = OptVol.index.get_loc(expdate)
            # taus = days diff + week adjustment
            taus_df[OptTicker].iloc[:date_iloc+1] = pd.Int64Index(data=range(date_iloc,-1,-1)) \
                            + week_adj*(Week_N[date_iloc] - Week_N[:date_iloc+1])
            
        if export2excel: self._export2excel_(taus, excelname)
        return taus_df
    
    def get_volume_bytau(self, OptVol, taus, export2excel=False, excelname=None):
        tau_effect = pd.DataFrame(index=taus)
        
        for OptTicker in self._chop_OptTickers_(OptVol.columns):
            expdate = OptTicker.split("_")[1]
            date_iloc = OptVol.index.get_loc(expdate)
            tau_effect[OptTicker] = OptVol[OptTicker].iloc[[date_iloc-x for x in taus]].values
        
        if export2excel: self._export2excel_(tau_effect, excelname)
        return tau_effect
        
    def concat_OptVolUnder_bytau(self, OptVol, Under, taus, data_columes, export2excel=False, excelname=None):
        if isinstance(taus, int): taus = np.array([taus]) 
        else: taus = np.array(taus)[::-1]
        if data_columes[0]!='OptVol': data_columes = ['OptVol']+data_columes
        # col_generator = [('OptVol' or colname in Under, shift, colname in data_bytaus)]
        col_generator = [] 
        for data_colume in data_columes:
            x = re.split(r'\(|\)|,|;|\[|\]',data_colume)
            # check if data_columes is valid
            if x[0] not in Under.columns and x[0]!='OptVol': raise ValueError('%s not found in Under.colume' %x[0])
            if len(x) == 1:
                col_generator += (x[0], 0, data_colume),
            else:
                col_generator += [(x[0], int(shift), x[0]+'(%s)'%shift if int(shift) else x[0]) for shift in x[1:-1]]

        # inner join OptVol with Under on index
        OptVolUnder = pd.concat([OptVol,Under],axis=1,join='inner')
        #         Under = Under.drop(Under.index.difference(OptVol.index))

        data_bytaus = []
        # for each OptTicker, derive dataframe data_OptTicker appended in data_bytaus
        for OptTicker in self._chop_OptTickers_(OptVol.columns):
            expdate = OptTicker.split("_")[1]
            date_iloc = OptVolUnder.index.get_loc(expdate)
            idxs = date_iloc - taus
            
            data_OptTicker = pd.DataFrame(index=OptVolUnder.index[idxs]) # index : AsOfDate
            data_OptTicker['OptTicker'] = OptTicker
            data_OptTicker['Tau'] = taus
            for colname, shift, colname_Output in col_generator:
                if colname == 'OptVol': colname = OptTicker
                data_OptTicker[colname_Output] = OptVolUnder[colname].iloc[idxs-shift].values

            data_bytaus += data_OptTicker,
        
        data_bytaus = pd.concat(data_bytaus,axis=0) # concatenate dataframes
        
        if export2excel: self._export2excel_(data_bytaus, excelname)
        return data_bytaus