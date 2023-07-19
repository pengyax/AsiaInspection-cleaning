import pandas as pd
import numpy as np

def pad_item(item):
        if str(item).isdigit() and len(str(item)) < 5:
            item = str(item).zfill(5)
        return item

def replace_defect_code(df):
    return df.replace({'Defect Code':{'.*Functional.*':'Functional', '.*Dimensional.*':'Dimensional', '.*Foreign Particulate.*':'Foreign Particulate', '.*Packaging/Labeling.*':'Labeling', '.*Visual.*':'Visual'}}, regex=True)

def assign_disposition_type(df):
    return df.assign(**{'Disposition Type' : lambda d : d['Disposition Type'].map({'Return to Supplier':'rework', 'Accept As Is':'deviation', 'Forward to QA':'deviation'}).fillna('unknown')})

def add_ins_data(df_inspection_status,df_ncmr,df_sharepoint,df_add,std):
    qim_his = df_add[df_add['Path'] == 'QIM']['ID'].to_list()
    global rename 
    rename = {'Inspection Number':'ID',
          'PO Number':'PO Number',
          'Lot Number':'Lot Number',
          'Supplier Number':'Vendor code',
          'Supplier Name':'Vendor',
          'Factory':'Factory',
          'Division':'Division',
          'Date':'Inspection Date',
          'Inspector Name':'Inspector',
          'Manufacturing_data':'Manufacture Date',
          'Part Number':'Item Number',
          'Total Quantity Received':'Qty EA',
          'Result':'Results',
          'Defect Code':'Reject Code',
          'Description':'Reject Description',
          'Comments':'Comments',
          'Created':'Created',
          'Created By':'Created By',
          'Modified':'Modified',
          'Modified By':'Modified By',
          'Path':'Path',
          'Current Phase':'Current Phase'
          }
    
    df_ncmr = (
        df_ncmr
        .query('`Current Phase` in ("Completed","Functional Review","Clinical Review","Disposition","Verification")')
        .pipe(assign_disposition_type)
        .pipe(replace_defect_code)
        .assign(Comments = lambda d : d['NCMR Number'].str.cat([d['Disposition Type'],d['Defect Code']],sep=','))
        .loc[:,['NCMR Number','Description','Defect Code','Disposition Type','Current Phase','Comments']]
)
    
    df_new = (
    df_inspection_status
    .query('`Re-Inspection` != "Yes" and Date >=@std')
    .drop_duplicates(keep='first')
    .assign(Result = df_inspection_status['Result'].map({'PASS':'A','FAIL':'R'}))
    .assign(Manufacturing_data = pd.to_datetime({'year':df_inspection_status['Manufacturing Year'], 'month':df_inspection_status['Manufacturing Month'], 'day':1}, format='%Y-%m'))
    .pipe(lambda d : pd.merge(d,df_ncmr,on='NCMR Number',how='left'))
    .assign(**{'Created' : None,
                'Created By' : None,
                'Modified' : None,
                'Modified By' : None,
                'Path' : 'QIM',
                'Factory' : None
                })
    .rename(columns = rename)
    .assign(Division = lambda d : d['Division'].str.replace('Division ',''))
    .loc[:,rename.values()]
    .query('ID not in @qim_his')
    .pipe(lambda d : pd.concat([d,df_sharepoint]))
    .drop(columns = ['Document Links','Item Type','Combine inspection'])
    .assign(**{'Item Number' : lambda d : d['Item Number'].str.strip()})
    .assign(**{"Item Number" : lambda d : d['Item Number'].apply(pad_item),
               'Reject Code' : lambda d : d['Reject Code'].mask(d['Results'] == "A",None),
               'PO Number' : lambda d : d['PO Number'].str.strip()
               })
    .query('ID not in (7923,1)')
    .query('~Inspector.str.contains("Charles",na = False)')
)
    return df_new

if __name__ == "__main__":
    
    df_inspection_status = pd.read_excel('../Inspection Status.xlsx',skiprows = 5)
    df_NCMR = pd.read_excel('../NCMR Status.xlsx',skiprows = 5)
    inspection_status_no = 71070
    std = '2022-01-01'
    df_sharepoint = pd.read_excel('../Book1.xlsx')
    
   
    df_2022 = pd.read_excel(r'C:\Medline\database\Asia Inspection Database\2022\QP-00017-F-00005 Asia Inspection Database 2022.XLSM',sheet_name="Sheet1",usecols="A,U")
    df_2023 = pd.read_excel(r'C:\Medline\database\Asia Inspection Database\2022\QP-00017-F-00005 Asia Inspection Database 2023.XLSM',sheet_name="Sheet1",usecols="A,U")
    df_add = pd.concat([df_2023,df_2022])
    add_ins_data(df_inspection_status,df_NCMR,df_sharepoint,df_add,std).to_excel('newadd.xlsx',index = False)