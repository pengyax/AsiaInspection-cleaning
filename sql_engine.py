from sqlalchemy import create_engine
import pymssql
import cx_Oracle
import openpyxl
from sshtunnel import SSHTunnelForwarder


def connect(type='new_ERP', echo=False):
    if type == 'fn_sql':
        engine = create_engine("mssql+pymssql://sa:123456@localhost:1433/BI", echo=echo)
    
    elif type == 'fn_mysql':
        engine = create_engine("mysql+pymysql://root:123456@localhost:3306/mydb", echo=echo)
		## ERP链接
    elif type=='biconfig':
        engine = create_engine("mssql+pymssql://biuser:bierp@!123456@139.196.55.81/BIConfig", echo=echo)
		
    elif type == 'JK':
        engine = create_engine("mssql+pymssql://biuser:bierp@!123456@139.196.55.81/JKERPDB_UE", echo=echo)
    elif type == 'bidata':
        engine = create_engine("mssql+pymssql://sa:bidata@!654321@47.100.200.9/BIdata", echo=echo, )
		## BI连接
    elif type == 'offline':
        engine = create_engine("mssql+pymssql://biuser:bierp@!123456@192.168.128.10/shcm", echo=echo)
    elif type == 'wms':
        engine = cx_Oracle.connect("WMS_BI/biwms@!123456@47.100.205.186/orcl")
		
		##库存链接
		
		
    elif type == 'drp':
        engine = create_engine("mssql+pymssql://biuser:bierp@!123456@139.196.55.81/SCLM_DRPDB", echo=echo)
    elif type == 'll':
        engine = create_engine("mysql+pymysql://sclm_ekp:Saselomo@2019@rm-uf6a5tuzxnij813e3bo.mysql.rds.aliyuncs.com:3306/sclm_ekp")
    elif type == 'pf':
        engine = create_engine(
            "mysql+pymysql://root:Lyq19900823!@47.102.154.75:3306/edb")
    elif type == 'ty':
        engine = create_engine("mssql+pymssql://biuser:bierp@!123456@139.196.55.81/TIANYEERPDB_UE", echo=echo)
    elif type == 'fr':
        engine = create_engine("mysql+pymysql://finedb_fine_user:2ranked$8drub$Lea5$Teem@rm-uf6a5tuzxnij813e3bo.mysql.rds.aliyuncs.com:3306/finedb")
    elif type=='old':
        engine = create_engine("mssql+pymssql://biuser:bierp@!123456@192.168.128.10/sclm0999", echo=echo)
    elif type=='yx':
        server = SSHTunnelForwarder(
            ("114.55.65.239", 22),  # B机器的配置
            ssh_username="sclm",
            ssh_pkey="./sclm.dat",
            remote_bind_address=("172.16.1.5", 3306))
        server.start()
        engine = create_engine(f"mysql+pymysql://oryx_canal:oryx_canal@@YouXin20201110@127.0.0.1:{server.local_bind_port}/oryx_bi")
		## 新erp有信
		
        return engine, server
    elif type=='pfsave_test':
        engine = create_engine("mssql+pymssql://sa:WHJM@@)!*1010@47.102.221.177/SCLMERPDB_UE", echo=echo)
    elif type=='pfsave':
        engine = create_engine("mssql+pymssql://biuser:bierp@!123456@139.196.55.81/BIConfig", echo=echo)
    else:
        engine = None
    return engine
