'''
Project Name	: GRT Jewel Price Data Aggregator                                                                         
Source  		: https://www.grtjewels.com/
Developer		: Subash KS                                                                                         
Developed date	: 2024-11-09                                                                                            
Description     : Scraping up-to-date data for Gold, Platinum, and Silver prices from the GRT website and calculating the daily price differences compared to the previous day.
'''

import requests
import os
import json
import datetime
import re
import pandas as pd

class scrapingClass:

    def __init__(self, configData):
        self.sess = requests.Session()
        self.currentTime = datetime.datetime.now()
        self.currentDate = self.currentTime.date()
        print(self.currentDate)
        self.fileDate = self.currentTime.strftime('%Y-%m-%d%H%M%S')
        cachePath = configData['GRT']['cachePath']
        if not os.path.exists(cachePath):
            os.makedirs(cachePath)
        self.cacheFile = f'{cachePath}GRT_Home_Page_{self.fileDate}.html'
        outputPath = configData['GRT']['outputPath']
        if not os.path.exists(outputPath):
            os.makedirs(outputPath)
        self.outputFileName = f'{outputPath}Jewel Prices.xlsx'
    

    def excelFormat(self):
        df = pd.read_excel(self.outputFileName, sheet_name='Data')
        
        with pd.ExcelWriter(self.outputFileName, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Data', index=False)

            workBook = writer.book
            workSheet = writer.sheets['Data']

            headerFormat = workBook.add_format({'bold': True, 'bg_color': '#F4B183', 'border': 1}) #orange
            workSheet.write_row(0, 0, df.columns, headerFormat)

            dateColumnFormat = workBook.add_format({'bg_color': '#B7DEE8'}) #blue
            diffColumnFormat = workBook.add_format({'bg_color': '#C6E0B4'}) #green
            dateTimeFormat = workBook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'})

            for colNum, col in enumerate(df.columns):
                for rowNum in range(1, len(df) + 1):
                    cellValue = df.iloc[rowNum - 1, colNum]
                    
                    if pd.notna(cellValue):
                        if col == "Date":
                            workSheet.write(rowNum, colNum, cellValue, dateColumnFormat)
                        elif "diff" in col.lower():
                            workSheet.write(rowNum, colNum, cellValue, diffColumnFormat)  
                        elif col == "Captured Time":  
                            workSheet.write(rowNum, colNum, cellValue, dateTimeFormat)
                        else:
                            workSheet.write(rowNum, colNum, cellValue)  
                            
                maxLen = max(df[col].astype(str).map(len).max(), len(col)) + 2
                workSheet.set_column(colNum, colNum, maxLen)
            workSheet.freeze_panes(1, 1)


    def readingBlock(self):
        headersDict = {
            'upgrade-insecure-requests':'1',
            'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
            }
        obj = self.sess.get(configData['GRT']['grtJewelsLink'], headers = headersDict)
        
        with open(self.cacheFile, 'wb') as fh:
            fh.write(obj.content)
        jewelStatus = re.search('<ul\s*class="state_rates">([\w\W]*?)<\/ul>', obj.text).group(1)
        
        self.dataDict = {}
        for jewel, karat, gram, rate in re.findall('<li>(\w+)\s*-\s*(?:(\d+k)\s*-\s*)?(\d+\s*g)\s*-\s*Rs(\d+)<\/li>', jewelStatus):
            if karat: 
                jewel = jewel+'/'+karat
            self.dataDict.setdefault(self.currentDate, {}).setdefault(jewel, {})[gram] = rate


    def writingBlock(self):
        
        try:
            df = pd.read_excel(self.outputFileName, sheet_name='Data')
        except FileNotFoundError:
            df = pd.DataFrame(columns=configData['GRT']['excelHeaders'])
            df.to_excel(self.outputFileName, sheet_name='Data', index=False)
            print('New Excel file created')


        if not df.empty:
            latestData = df.iloc[-1]

            diffOfG24 = latestData['24K GOLD/1g'] - int(self.dataDict[self.currentDate]['GOLD/24k']['1 g'])
            if latestData['24K GOLD/1g'] > int(self.dataDict[self.currentDate]['GOLD/24k']['1 g']):
                symOfG24 = '↓'
            elif latestData['24K GOLD/1g'] < int(self.dataDict[self.currentDate]['GOLD/24k']['1 g']):
                symOfG24 = '↑'
            else:
                symOfG24 = '⏸️'
            
            diffOfG22 = latestData['22K GOLD/1g'] - int(self.dataDict[self.currentDate]['GOLD/22k']['1 g'])
            if latestData['22K GOLD/1g'] > int(self.dataDict[self.currentDate]['GOLD/22k']['1 g']):
                symOfG22 = '↓'
            elif latestData['22K GOLD/1g'] < int(self.dataDict[self.currentDate]['GOLD/22k']['1 g']):
                symOfG22 = '↑'
            else:
                symOfG22 = '⏸️'
            
            diffOfG18 = latestData['18K GOLD/1g'] - int(self.dataDict[self.currentDate]['GOLD/18k']['1 g'])
            if latestData['18K GOLD/1g'] > int(self.dataDict[self.currentDate]['GOLD/18k']['1 g']):
                symOfG18 = '↓'
            elif latestData['18K GOLD/1g'] < int(self.dataDict[self.currentDate]['GOLD/18k']['1 g']):
                symOfG18 = '↑'
            else:
                symOfG18 = '⏸️'
            
            diffOfP = latestData['PLATINUM/1g'] - int(self.dataDict[self.currentDate]['PLATINUM']['1 g'])
            if latestData['PLATINUM/1g'] > int(self.dataDict[self.currentDate]['PLATINUM']['1 g']):
                symOfP = '↓'
            elif latestData['PLATINUM/1g'] < int(self.dataDict[self.currentDate]['PLATINUM']['1 g']):
                symOfP = '↑'
            else:
                symOfP = '⏸️'
            
            diffOfS = latestData['SILVER/1g'] - int(self.dataDict[self.currentDate]['SILVER']['1 g'])
            if latestData['SILVER/1g'] > int(self.dataDict[self.currentDate]['SILVER']['1 g']):
                symOfS = '↓'
            elif latestData['SILVER/1g'] < int(self.dataDict[self.currentDate]['SILVER']['1 g']):
                symOfS = '↑'
            else:
                symOfS = '⏸️'
            
        else:
            latestData = None
            diffOfG24 = diffOfG22 = diffOfG18 = diffOfP = diffOfS = ''
            symOfG24 = symOfG22 = symOfG18 = symOfP = symOfS = ''
            
        excelData = {
            'Date' : self.currentTime.strftime('%Y-%m-%d'),
            
            '24K GOLD/1g' : self.dataDict[self.currentDate]['GOLD/24k']['1 g'],
            '24K GOLD/1g Diff' : symOfG24+str(diffOfG24),
            '24K GOLD/8g' : int(self.dataDict[self.currentDate]['GOLD/24k']['1 g'])*8,
            '24K GOLD/8g Diff' : symOfG24+str(diffOfG24*8),
            
            '22K GOLD/1g' : self.dataDict[self.currentDate]['GOLD/22k']['1 g'],
            '22K GOLD/1g Diff' : symOfG22+str(diffOfG22),
            '22K GOLD/8g' : int(self.dataDict[self.currentDate]['GOLD/22k']['1 g'])*8,
            '22K GOLD/8g Diff' : symOfG22+str(diffOfG22*8),
            
            '18K GOLD/1g' : self.dataDict[self.currentDate]['GOLD/18k']['1 g'],
            '18K GOLD/1g Diff' : symOfG18+str(diffOfG18),
            '18K GOLD/8g' : int(self.dataDict[self.currentDate]['GOLD/18k']['1 g'])*8,
            '18K GOLD/8g Diff' : symOfG18+str(diffOfG18*8),
            
            'PLATINUM/1g' : self.dataDict[self.currentDate]['PLATINUM']['1 g'],
            'PLATINUM/1g Diff' : symOfP+str(diffOfP),
            'PLATINUM/8g' : int(self.dataDict[self.currentDate]['PLATINUM']['1 g'])*8,
            'PLATINUM/8g Diff' : symOfP+str(diffOfP*8),
            
            'SILVER/1g' : self.dataDict[self.currentDate]['SILVER']['1 g'],
            'SILVER/1g Diff' : symOfS+str(diffOfS),
            'SILVER/8g' : int(self.dataDict[self.currentDate]['SILVER']['1 g'])*8,
            'SILVER/8g Diff' : symOfS+str(diffOfS*8),
            
            'Captured Time' : self.currentTime
        }
        
        if latestData is not None:
            if (latestData['Date'] != str(self.currentDate)) or (str(latestData['24K GOLD/1g']) != self.dataDict[self.currentDate]['GOLD/24k']['1 g'] or str(latestData['22K GOLD/1g']) != self.dataDict[self.currentDate]['GOLD/22k']['1 g'] or str(latestData['18K GOLD/1g']) != self.dataDict[self.currentDate]['GOLD/18k']['1 g'] or str(latestData['PLATINUM/1g']) != self.dataDict[self.currentDate]['PLATINUM']['1 g'] or str(latestData['SILVER/1g']) != self.dataDict[self.currentDate]['SILVER']['1 g']):
                df = pd.concat([df, pd.DataFrame([excelData])], ignore_index=True)
                df.to_excel(self.outputFileName, sheet_name='Data', index=False)
                print(f'New values got concatenated for {self.currentDate}')
            else:
                print(f'New values already exist for {self.currentDate}')
                
        else:
            df = pd.concat([df, pd.DataFrame([excelData])], ignore_index=True)
            df.to_excel(self.outputFileName, sheet_name='Data', index=False)
            print(f'New values got concatenated for {self.currentDate}')
            
        self.excelFormat()


if __name__ == '__main__':
    
    with open('jsonConfig.json', 'r', encoding = 'utf-8') as fh:
        configData = json.load(fh)
    
    if not configData['scriptRunStatus']:
        sys.exit(1)
        
    classOj = scrapingClass(configData)
    classOj.readingBlock()
    classOj.writingBlock()