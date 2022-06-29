import math
import tushare as ts
import numpy as np
import pandas as pd
import time
import datetime

# ---------------------------------------------------
# set Token of Tushare
token = 'abcd'
ts.set_token(token)

class getStockData():
    def __init__(self, endDateSpec, stockListPath, market, preDataPath):
        '''
        stockCode: the code of stock
        stockStartDate: the start date to go public
        stardDate: the date last updated
        endDate: today
        Market: if SHA: SH-A股
                elif SHB: SH-B股
                elif SHK: SH-科创板
                elif SZA: SZ-A股
                elif SZB: SZ-B股
                elif SZC: SZ-创业板
                elif BJ: Beijing
        firstRun: if True: get data first time; else: not the first time
        '''

        self.stockListPath = stockListPath
        self.market = market
        self.endDateSpec = endDateSpec
        self.preDataPath=preDataPath

    def stockListToData(self):
        # download data from Shanghai stock market
        if self.market in ['SHA', 'SHB', 'SHK']:
            # read stock list file
            stockList = pd.read_csv(self.stockListPath, dtype={'Stock_Code': str})
            stockList['Start_Date'] = pd.to_datetime(stockList['Start_Date'])
            stockList['Start_Date'] = stockList['Start_Date'].dt.strftime('%Y%m%d')

            for i in range(0, stockList.shape[0]):
                # get stockCode string
                stockCodeTushare = stockList['Stock_Code'][i] + '.SH'

                # get the fist day to public
                stockStartDate = stockList['Start_Date'][i]

                # get info in current file stored on local machine
                preStartDate, preEndDate, preStockFilePath = self.checkPreData(stockCodeTushare)

                if preStartDate == 0:
                    # now previous data for the code, so download all the new data
                    startDate = stockStartDate
                    endDate = self.endDateSpec
                    stockData = self.getTushareData(stockCodeTushare, startDate, endDate)

                elif preStartDate > stockStartDate:
                    # download data segment and as supplement
                    startDate = stockStartDate
                    endDate = preStartDate
                    stockDataSegment = self.getTushareData(stockCodeTushare, startDate, endDate)
                    preStockData = pd.read_excel(preStockFilePath, engine='openpyxl')
                    stockData = stockDataSegment.append(preStockData)

                elif preEndDate < self.endDateSpec:
                    startDate = preEndDate
                    endDate = self.endDateSpec
                    preStockData = pd.read_excel(preStockFilePath, engine='openpyxl')
                    stockDataSegment = self.getTushareData(stockCodeTushare, startDate, endDate)
                    stockData = preStockData.append(stockDataSegment)

                print('stockCodeTushare', stockCodeTushare)
                stockData.to_csv(preStockFilePath)
                time.sleep(2)

        # download data from Shenzhen stock market
        elif self.market in ['SZA', 'SZB', 'SZC']:
            # read stock list file
            stockList = pd.read_excel(self.stockListPath, dtype={'A股代码': str}, engine='openpyxl')
            stockList['A股上市日期'] = pd.to_datetime(stockList['A股上市日期'])
            stockList['A股上市日期'] = stockList['A股上市日期'].dt.strftime('%Y%m%d')

            for i in range(0, stockList.shape[0]):
                # get stockCode string
                stockCodeTushare = stockList['A股代码'][i] + '.SZ'

                # get the fist day to public
                stockStartDate = stockList['A股上市日期'][i]

                # get info in current file stored on local machine
                preStartDate, preEndDate, preStockFilePath = self.checkPreData(stockCodeTushare)

                if preStartDate == 0:
                    # now previous data for the code, so download all the new data
                    startDate = stockStartDate
                    endDate = self.endDateSpec
                    stockData = self.getTushareData(stockCodeTushare, startDate, endDate)

                elif preStartDate > stockStartDate:
                    # download data segment and as supplement
                    startDate = stockStartDate
                    endDate = preStartDate
                    stockDataSegment = self.getTushareData(stockCodeTushare, startDate, endDate)
                    preStockData = pd.read_excel(preStockFilePath, engine='openpyxl')
                    stockData = stockDataSegment.append(preStockData)

                elif preEndDate < self.endDateSpec:
                    startDate = preEndDate
                    endDate = self.endDateSpec
                    preStockData = pd.read_excel(preStockFilePath, engine='openpyxl')
                    stockDataSegment = self.getTushareData(stockCodeTushare, startDate, endDate)
                    stockData = preStockData.append(stockDataSegment)

                print('stockCodeTushare', stockCodeTushare)
                stockData.to_csv(preStockFilePath)
                time.sleep(2)

    def checkPreData(self, stockCode):

        preStockFilePath = self.preDataPath + '\\' + '{}'.format(stockCode) + '.csv'

        try:
            preStockData = pd.read_csv(preStockFilePath)
            preStartDate = preStockData.index.min()
            preEndDate = preStockData.index.max()
            if preStartDate == preEndDate:
                preStartDate = 0
                preEndDate = 0
                print("Only one line of data in previous file")
        except:
            print("Can't find" + '{}'.format(stockCode)+'.csv')
            preStartDate = 0
            preEndDate = 0

        return preStartDate, preEndDate, preStockFilePath

    def getTushareData(self, stockCode, startDate, endDate):
        # interface of tushare
        pro = ts.pro_api()

        # get stock data from tushare
        data = pro.query('daily', ts_code=stockCode,
                         start_date=startDate, end_date=endDate)

        print(data.head())

        data = data.sort_values(by='trade_date', ascending=True)
        data = data.drop(columns='ts_code')
        data.rename(columns={'trade_date': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low',
                             'close': 'Close', 'pre_close': 'Pre Close', 'change': 'Change',
                             'pct_chg': 'Pct Change', 'vol': 'Volume', 'amount': 'Amount'}, inplace=True)
        print(data.head())
        data['Date'] = pd.to_datetime(data['Date'])
        data = data.set_index('Date', drop=True)
        data['Adj Open'] = data['Open']
        data['Adj Close'] = data['Close']

        # print(data.head(5))
        return data

if __name__ == '__main__':
    ###### download stock data of Shanghai A
    marketType = 'SHA'
    stockListPath = r"C:\Users\Felix\PycharmProjects\stockDataDownloader\sh_list.csv"
    preDataPath = "C:\\Users\\Felix\\PycharmProjects\\stockDataDownloader\\stock_data\\SHSE"
    dateNow = datetime.datetime.now()
    dateNow = dateNow.strftime('%Y%m%d')
    endDate = int(dateNow)
    getData = getStockData(endDate, stockListPath, marketType, preDataPath)
    getData.stockListToData()

    ###### download stock data of Shenzhen A
    marketType='SZA'
    stockListPath= r"C:\Users\Felix\PycharmProjects\stockDataDownloader/sz_list.xlsx"
    preDataPath = "C:\\Users\\Felix\\PycharmProjects\\stockDataDownloader\\stock_data\\SZSE"
    dateNow = datetime.datetime.now()
    dateNow = dateNow.strftime('%Y%m%d')
    endDate = int(dateNow)
    getData = getStockData(endDate, stockListPath, marketType, preDataPath)
    getData.stockListToData()