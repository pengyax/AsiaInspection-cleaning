import pandas as pd
import numpy as np
import database_cleaning as dc
from sql_engine import connect
from datetime import datetime


def out_status(df_add,df_product_family):
    df = df_add.copy()
    df['Inspection Date'] = pd.to_datetime(df['Inspection Date'])
    print(df.info())
    df_product_family = (
    df_product_family
    .assign(**{
        "Material Number" : lambda d : d['Material Number'].map(str).str.upper().str.strip(),
        "Product Family" : lambda d : d['Product Family'].str.upper().str.strip()
    })
    .drop_duplicates(subset=['Material Number'],keep='last')
)
    df_uninspection = df.loc[df['Results'].isin(['G','W'])]
    
    df_inspection = (
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
    df_inspection
    .query('Path.str.startswith("QIM",na=False)')
    .sort_values(['ID','Inspection Date'],ascending=[False,False])
    .drop_duplicates(subset=['ID'],keep='last')
)
    
    df_offline_duplicate = (
    df_inspection
    .query('~Path.str.startswith("QIM",na=False)')
    .sort_values(['ID','Inspection Date'],ascending=[False,False])
    .drop_duplicates(subset=['Lot Number','Vendor','Item Number','Inspector','Inspection Date'],keep='last')
)

    df_duplicate = pd.concat([df_QIM_duplicate,df_offline_duplicate])
    # df_duplicate['Inspection Date'] = pd.to_datetime(df_duplicate['Inspection Date'])
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

    print('Audit Status完成')
    print('===='*6)
        
    list1 = ['NONEX0023FALLMED INDUSTRIAL LIMITED']    
    df_reject = (
    df_duplicate
    .sort_values(['item_key','Inspection Date',],ascending=[False,False])
    .groupby(['item_key'])[['item_key','Inspection Date','Results','Reject Code']].head(5)
    .query('`Reject Code` == "Functional" and item_key not in @list1')
    .loc[:,'item_key']
    .drop_duplicates() 
)   
    print('rej_key完成')
    print('===='*6)
    
    
    df_base = pd.concat([df_duplicate,df_uninspection],ignore_index=True)
    
    df_timeDiff = (
    df_base
    .sort_values(['Vendor','Inspection Date'],ascending=[False,False])
    .reset_index(drop=True)
    .assign(Time_Diff = lambda d : d.groupby('Vendor')['Inspection Date'].diff().dt.days.abs())
    .assign(Time_Diff = lambda d : d['Time_Diff'].fillna((datetime.today() - d["Inspection Date"]).dt.days))
)
    idx = df_timeDiff.groupby("Vendor").apply(lambda x: x[x["Time_Diff"] > 90].index[0] if any(x["Time_Diff"] > 90) else None)
    
    df_overdue = (
    df_timeDiff
    .loc[idx.dropna()]
    .reset_index()
    .pipe(lambda d : pd.merge(d,df_timeDiff.groupby("Vendor").head(1).reset_index(),how='left',on=['Vendor']))
    .assign(Count = lambda d : d['index_x'] - d['index_y'] + 1)
    .query('Count < 5')
    .loc[:,['Vendor','Time_Diff_x','Inspection Date_x','Inspection Date_y','Count']]
)
    print('overdue完成')
    print('===='*6)
    
    df_unNewItem = (
    df_duplicate
    .query('Results in ("A","R")')
    .groupby(['Vendor','Item Number'],as_index=False).size()
    .query('size >= 5')
    .assign(**{
        "newItem" : lambda d : d['Item Number'].str.cat(d['Vendor'])
        })
)
    
    
    with pd.ExcelWriter('../output/Audit Status.xlsx') as writer:
        df_item.to_excel(writer,sheet_name='items')
        df_product.to_excel(writer,sheet_name='product_family')
        df_reject.to_excel(writer,sheet_name='rek_key')
        df_overdue.to_excel(writer,sheet_name='overdue')
        df_unNewItem.to_excel(writer,sheet_name='unNewItem')
    print('写入完成')
    print('===='*6)

if __name__ == "__main__":
    
    # df_inspection_status = pd.read_excel('../Inspection Status.xlsx',skiprows = 5)
    # df_NCMR = pd.read_excel('../NCMR Status.xlsx',skiprows = 5)
    # inspection_status_no = 71070
    # std = '2022-01-01'
    # df_sharepoint = pd.read_excel('../Book1.xlsx')
    fn_engine = connect('fn_mysql')
    
    sql_query = f'''
                select
                    ID,
                    `PO Number`,
                    `Lot Number`,
                    `Vendor Code`,
                    Vendor,
                    Division,
                    `Inspection Date`,
                    Inspector,
                    `Item Number`,
                    Results,
                    `Reject Code`,
                    Path
                from
                    inspection_data_all
                '''
    df = pd.read_sql(sql_query,fn_engine)
    print('加载历史数据完成')
    print('===='*6)
    
    df_product_family = pd.read_excel('../output/product_family.xlsx',sheet_name=0)
    # df_new = pd.read_excel('../output/addNew.xlsx')
    
    print('运算开始')
    print('===='*6)
    out_status(df,df_product_family)
    print('完成')