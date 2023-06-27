import pandas as pd
import numpy as np
import database_cleaning as dc


def out_status(df_add,df_new,df_product_family):
    df = pd.concat([df_add,df_new],ignore_index=True)
    
    df_product_family = (
    df_product_family
    .assign(**{
        "Material Number" : lambda d : d['Material Number'].map(str).str.upper().str.strip(),
        "Product Family" : lambda d : d['Product Family'].str.upper().str.strip()
    })
    .drop_duplicates(subset=['Material Number'],keep='last')
)
    
    df = (
    df
    .query('Results in ("A","R")')
    .assign(**{
        "Item Number" : lambda d : d['Item Number'].map(str).str.upper().str.strip(),
        "Vendor" : lambda d : d['Vendor'].str.strip()
    })
    .pipe(lambda d : pd.merge(d,df_product_family,left_on='Item Number',right_on='Material Number',how='left'))
    .assign(
        item_key = lambda d : d['Item Number'].map(str)+d['Vendor'].map(str),
        product_family_key = lambda d : d['Product Family'].map(str)+d['Vendor'].map(str)
    )
)
    
    df_QIM_duplicate = (
    df
    .query('Path.str.startswith("QIM",na=False)')
    .sort_values(['ID','Inspection Date'],ascending=[False,False])
    .drop_duplicates(subset=['ID'],keep='last')
)
    
    df_offline_duplicate = (
    df
    .query('~Path.str.startswith("QIM",na=False)')
    .sort_values(['ID','Inspection Date'],ascending=[False,False])
    .drop_duplicates(subset=['Lot Number','Vendor','Item Number','Inspector','Inspection Date'],keep='last')
)

    df_duplicate = pd.concat([df_QIM_duplicate,df_offline_duplicate])
    print('去重完成')
    print('===='*6)

    df_item = (
    df_duplicate
    .sort_values(['item_key','Inspection Date',],ascending=[False,False])
    .groupby(['item_key'])[['item_key','Inspection Date','Results']].head(5)
    .query('Results == "A"')
    .groupby('item_key').size().reset_index()
    .set_axis(['item_key','count'],axis=1)
    .assign(judge = lambda d : d['count'].apply(lambda s :'N' if s<5 else 'Y' ))
    .iloc[:,[0,2]]
)
    
    df_product = (
    df_duplicate
    .sort_values(['product_family_key','Inspection Date',],ascending=[False,False])
    .groupby(['product_family_key'])[['product_family_key','Inspection Date','Results']].head(5)
    .query('Results == "A"')
    .groupby('product_family_key').size().reset_index()
    .set_axis(['product_family_key','count'],axis=1)
    .assign(judge = lambda d : d['count'].apply(lambda s :'N' if s<5 else 'Y' ))
    .iloc[:,[0,2]]
)

    with pd.ExcelWriter('../output/Audit Status.xlsx') as writer:
        df_item.to_excel(writer,sheet_name='items')
        df_product.to_excel(writer,sheet_name='product_family')
    print('Audit Status完成')
    print('===='*6)
        
    df_reject = (
    df_duplicate
    .sort_values(['item_key','Inspection Date',],ascending=[False,False])
    .groupby(['item_key'])[['item_key','Inspection Date','Results','Reject Code']].head(5)
    .query('`Reject Code` == "Functional"')
    .loc[:,'item_key']
    .drop_duplicates()
)   
    df_reject.to_excel('../output/rej_key.xlsx')
    print('rej_key完成')
    print('===='*6)

if __name__ == "__main__":
    
    # df_inspection_status = pd.read_excel('../Inspection Status.xlsx',skiprows = 5)
    # df_NCMR = pd.read_excel('../NCMR Status.xlsx',skiprows = 5)
    # inspection_status_no = 71070
    # std = '2022-01-01'
    # df_sharepoint = pd.read_excel('../Book1.xlsx')
    
    df_2018 = pd.read_excel(r'C:\Medline\database\Asia Inspection Database\2022\QP-00017-F-00005 Asia Inspection Database 2018.XLSM',sheet_name="Sheet1")
    df_2019 = pd.read_excel(r'C:\Medline\database\Asia Inspection Database\2022\QP-00017-F-00005 Asia Inspection Database 2019.XLSM',sheet_name="Sheet1")
    df_2020 = pd.read_excel(r'C:\Medline\database\Asia Inspection Database\2022\QP-00017-F-00005 Asia Inspection Database 2020.XLSM',sheet_name="Sheet1")
    df_2021 = pd.read_excel(r'C:\Medline\database\Asia Inspection Database\2022\QP-00017-F-00005 Asia Inspection Database 2021.XLSM',sheet_name="Sheet1")
    df_2022 = pd.read_excel(r'C:\Medline\database\Asia Inspection Database\2022\QP-00017-F-00005 Asia Inspection Database 2022.XLSM',sheet_name="Sheet1")
    df_2023 = pd.read_excel(r'C:\Medline\database\Asia Inspection Database\2022\QP-00017-F-00005 Asia Inspection Database 2023.XLSM',sheet_name="Sheet1")
    df_add = pd.concat([df_2023,df_2022,df_2021,df_2020,df_2019,df_2018]) 
    print('加载历史数据完成')
    print('===='*6)
    
    df_product_family = pd.read_excel('../output/product_family.xlsx',sheet_name=0)
    df_new = pd.read_excel('../output/addNew.xlsx')
    
    print('运算开始')
    print('===='*6)
    out_status(df_add,df_new,df_product_family)
    print('完成')