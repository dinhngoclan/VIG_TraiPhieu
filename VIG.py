# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np

xls = pd.ExcelFile(r'C:\Users\PC\Desktop\VIG3.xlsx')
VIG_mua = pd.read_excel(xls, 'VIG mua')
VIG_ban = pd.read_excel(xls, 'VIG ban')


mua = VIG_mua.groupby([r'NGAY_CHUYEN_TIEN', r'Số HĐ', r'MA_TP']).agg({'SỐ LƯỢNG': ['sum'], 'GIÁ GIAO DỊCH GROSS':['sum']})
mua.columns = ['SỐ LƯỢNG', r'GIÁ GIAO DỊCH GROSS']
mua = mua.reset_index()
mua['cumulative_TongSoLuongMua'] = mua.groupby([r'MA_TP'])['SỐ LƯỢNG'].apply(lambda x: x.cumsum())
mua['GIÁ GIAO DỊCH GROSS'] = mua['GIÁ GIAO DỊCH GROSS'].astype(np.int64)
mua['cumulative_GiaGiaoDichGrossMua'] = mua.groupby([r'MA_TP'])['GIÁ GIAO DỊCH GROSS'].apply(lambda x: x.cumsum())

ban = VIG_ban.groupby([r'Ngày giao dịch chuyển nhượng', r'Số HĐ', r'MÃ TP']).agg({'SỐ LƯỢNG': ['sum'], 'GIÁ GIAO DỊCH GROSS':['sum']})
ban.columns = ['SỐ LƯỢNG', r'GIÁ GIAO DỊCH GROSS']
ban = ban.reset_index()
ban['cumulative_TongSoLuongBan'] = ban.groupby([r'MÃ TP'])['SỐ LƯỢNG'].apply(lambda x: x.cumsum())
ban['cumulative_GiaGiaoDichGrossBan'] = ban.groupby([r'MÃ TP'])['GIÁ GIAO DỊCH GROSS'].apply(lambda x: x.cumsum())

test = pd.merge(ban, mua, left_on=[r'MÃ TP'], right_on = [r'MA_TP'], how='left')
test = test[(test[r'MÃ TP'] == test[r'MA_TP'])&(test[r'Ngày giao dịch chuyển nhượng'] >= test[r'NGAY_CHUYEN_TIEN'])]
test['cumulative_TongSoLuongThucCon'] = test['cumulative_TongSoLuongMua'] - test['cumulative_TongSoLuongBan']
test['cumulative_GiaGiaoDichGrossBan-Mua'] = test['cumulative_GiaGiaoDichGrossBan'] - test['cumulative_GiaGiaoDichGrossMua']
test['ThucCon'] = ""
test['Remove'] = ""
test['SoLgDungDeBan'] = ""
test.reset_index(drop=True, inplace=True)

offsetDict = {test.loc[0,'Số HĐ_y']:test.loc[0,r'SỐ LƯỢNG_y']}
soLuongBan = test.loc[0,r'SỐ LƯỢNG_x']
hopDongBan = test.loc[0,r'Số HĐ_x']
for index, row in test.iterrows():
    hopDongMua = row['Số HĐ_y']
    if hopDongBan != row[r'Số HĐ_x']:
        hopDongBan = row[r'Số HĐ_x']
        soLuongBan = row[r'SỐ LƯỢNG_x']
    if soLuongBan == 0:      
        test.at[index,'SoLgDungDeBan'] = 0
        if hopDongMua in offsetDict.keys():
            test.at[index,'ThucCon'] = offsetDict.get(hopDongMua)
            if offsetDict.get(hopDongMua) == 0:
                test.at[index,'Remove'] = "x"
        else:
            test.at[index,'ThucCon'] = offsetDict.get(hopDongMua,row[r'SỐ LƯỢNG_y'])
    else: #soLuongBan > 0
        if hopDongMua in offsetDict.keys() and offsetDict.get(hopDongMua) > 0:
            temp = offsetDict.get(hopDongMua) - soLuongBan
            if temp >= 0:
                test.at[index,'SoLgDungDeBan'] = soLuongBan
                soLuongBan = 0
                offsetDict[hopDongMua] = temp
                test.at[index,'ThucCon'] = temp
            else:
                test.at[index,'SoLgDungDeBan'] = offsetDict.get(hopDongMua)
                temp = soLuongBan - offsetDict.get(hopDongMua)
                soLuongBan = temp
                offsetDict[hopDongMua] = 0
                test.at[index,'ThucCon'] = 0
        elif hopDongMua not in offsetDict:
            temp = row[r'SỐ LƯỢNG_y']
            temp = temp - soLuongBan           
            if temp >= 0:
                test.at[index,'SoLgDungDeBan'] = soLuongBan
                soLuongBan = 0
                offsetDict[hopDongMua] = temp
                test.at[index,'ThucCon'] = temp
            else:
                test.at[index,'SoLgDungDeBan'] = row[r'SỐ LƯỢNG_y']
                soLuongBan = -temp
                offsetDict[hopDongMua] = 0
                test.at[index,'ThucCon'] = 0
        elif offsetDict.get(hopDongMua) == 0:
            test.at[index,'SoLgDungDeBan'] = 0
            test.at[index,'Remove'] = "x"
            test.at[index,'ThucCon'] = 0

    #if test.at[index,'Remove'] != "x":
    #    test.at[index,'SoLgBan'] = row[r'SỐ LƯỢNG_y'] - test.at[index,'ThucCon']


#Calculate lời lãi
test[r'Lời Lãi'] = test['SoLgDungDeBan']*(test[r'GIÁ GIAO DỊCH GROSS_x']/test[r'SỐ LƯỢNG_x'] - test[r'GIÁ GIAO DỊCH GROSS_y']/test[r'SỐ LƯỢNG_y'])
#test.to_excel(r'C:\Users\PC\Desktop\output3.xlsx')

##########################################
#Tách tách


onlyBuyDf = test.copy()
onlyBuyDf = onlyBuyDf[onlyBuyDf[r'NGAY_CHUYEN_TIEN'] < onlyBuyDf[r'Ngày giao dịch chuyển nhượng']]
onlyBuyDf[r'Ngày Giao Dịch'] = onlyBuyDf[r'NGAY_CHUYEN_TIEN']
onlyBuyDf = onlyBuyDf[onlyBuyDf['Remove'] != "x"]
onlyBuyDf['ThucCon'] = onlyBuyDf['ThucCon'] + onlyBuyDf['SoLgDungDeBan']
onlyBuyDf[r'Loại Giao Dịch']= "Buy"

# =============================================================================
# onlySellDf = test.copy()
# onlySellDf[r'Ngày Giao Dịch'] = onlySellDf[r'Ngày giao dịch chuyển nhượng']
# onlySellDf = onlySellDf[onlySellDf['Remove'] != "x"]
# onlySellDf = onlySellDf.groupby([r'Ngày Giao Dịch', r'MA_TP', r'Số HĐ_y', r'SỐ LƯỢNG_y', r'GIÁ GIAO DỊCH GROSS_y']).agg({'ThucCon': ['min']})
# onlySellDf.columns = ['ThucCon']
# onlySellDf = onlySellDf.reset_index()
# onlySellDf[r'Loại Giao Dịch']= "Sell"
# =============================================================================

buySellMixedDf = test.copy()
buySellMixedDf = buySellMixedDf[buySellMixedDf['Remove'] != "x"]
buySellMixedDf[r'Ngày Giao Dịch'] = buySellMixedDf[r'Ngày giao dịch chuyển nhượng']
buySellMixedDf[r'Loại Giao Dịch'] = "Sell"
#buySellMixedDf[r'Note'] = ""
for index, row in buySellMixedDf.iterrows():
    if row[r'Ngày giao dịch chuyển nhượng'] == row[r'NGAY_CHUYEN_TIEN']: #ngày bán == ngày mua
        if row[r'SoLgDungDeBan'] == 0 and row[r'SỐ LƯỢNG_y'] == row[r'ThucCon']:
            buySellMixedDf.at[index, r'Loại Giao Dịch'] = "Buy"
        elif row[r'SoLgDungDeBan'] > 0 and (row[r'SoLgDungDeBan'] + row[r'ThucCon']) == row[r'SỐ LƯỢNG_y']: #==SoLgMua
            buySellMixedDf.at[index, r'Loại Giao Dịch'] = "Buy"
        #row[r'Note'] = "BuyThenSellInDate" #due with this type later
buySellMixedDf = buySellMixedDf.groupby([r'Ngày Giao Dịch', r'MA_TP', r'Số HĐ_y', r'SỐ LƯỢNG_y', r'GIÁ GIAO DỊCH GROSS_y', r'Loại Giao Dịch']).agg({'ThucCon': ['min']})
buySellMixedDf.columns = ['ThucCon']
buySellMixedDf = buySellMixedDf.reset_index()
        
onlyBuyDf = pd.concat([onlyBuyDf, buySellMixedDf[buySellMixedDf['Loại Giao Dịch'] == "Buy"]])     
onlyBuyDf = onlyBuyDf.groupby([r'Ngày Giao Dịch', r'MA_TP', r'Số HĐ_y', r'SỐ LƯỢNG_y', r'GIÁ GIAO DỊCH GROSS_y']).agg({'ThucCon': ['max']})
onlyBuyDf.columns = ['ThucCon']
onlyBuyDf = onlyBuyDf.reset_index()
onlyBuyDf[r'Loại Giao Dịch']= "Buy"

onlySellDf = buySellMixedDf[buySellMixedDf['Loại Giao Dịch'] == "Sell"]
# =============================================================================
# onlySellDf = onlySellDf.groupby([r'Ngày Giao Dịch', r'MA_TP', r'Số HĐ_y', r'SỐ LƯỢNG_y', r'GIÁ GIAO DỊCH GROSS_y']).agg({'ThucCon': ['min']})
# onlySellDf.columns = ['ThucCon']
# onlySellDf = onlySellDf.reset_index()
# onlySellDf[r'Loại Giao Dịch']= "Sell"
# =============================================================================

buySellDf = pd.concat([onlyBuyDf, onlySellDf])
buySellDf[r'Giá Trị Tồn'] = buySellDf[r'ThucCon']/buySellDf[r'SỐ LƯỢNG_y']*buySellDf[r'GIÁ GIAO DỊCH GROSS_y']
buySellDf = buySellDf.sort_values(by=[r'Ngày Giao Dịch', r'MA_TP', r'Số HĐ_y'])
buySellDf.reset_index(drop=True, inplace=True)
for index, row in buySellDf.iterrows():
    if row[r'Loại Giao Dịch'] == "Buy" and row[r'SỐ LƯỢNG_y'] > row[r'ThucCon']:
        buySellDf.at[index, r'Loại Giao Dịch'] = "BuyThenSellInDate"
buySellDf.to_excel(r'C:\Users\PC\Desktop\buySellDf2.xlsx')
    	
test.to_excel(r'C:\Users\PC\Desktop\output3.xlsx')



