import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def active_data():
    # leo los archivos 
    advertisers = pd.read_csv('bucket/advertiser_ids')
    ads = pd.read_csv('bucket/ads_views')
    products = pd.read_csv('bucket/product_views')

    # filtro los archivos por actice advertiser
    active_ads = ads.merge(right=advertisers, how='inner', on='advertiser_id')
    active_products = products.merge(right=advertisers, how='inner', on='advertiser_id')
    
    # filtro los archivos por la fecha 
    today = datetime.today()
    recent_dates = today - timedelta(days=4)

    active_ads = active_ads[(pd.to_datetime(active_ads['date']) >= recent_dates) & (pd.to_datetime(active_ads['date']) <= today)]
    active_products = active_products[(pd.to_datetime(active_products['date']) >= recent_dates) & (pd.to_datetime(active_products['date']) <= today)]
    
    # guardo los archivos en el bucket
    active_ads.to_csv('bucket/active_ads.csv',index=False)
    active_products.to_csv('bucket/active_products.csv', index=False)
    return

def top_ctr():
    # Leo el archivo de ads activos
    ads = pd.read_csv('bucket/active_ads.csv')
    # Cambio el click por 1 y la impresión por 0 y agrupo para generar un nuevo df
    ads['type'] = ads['type'].apply(lambda x: 1 if x == 'click' else 0)
    ctr = ads.groupby(['advertiser_id','product_id']).agg({'type': 'mean'}).rename(columns={'type':'ctr'}).reset_index()
    
    # ordeno y filtro el top 20 de cada advertiser y lo guardo en el bucket
    top = ctr.sort_values(['advertiser_id','ctr'], ascending=[True,False]).groupby('advertiser_id').head(20)
    top.to_csv('bucket/top_ctr.csv', index=False)
    return



def top_prod():
    # Leo el archivo de los productos activos 
    prods = pd.read_csv('bucket/active_products.csv')

    # Cuento la cantidad de registros por la combinacion advertiser product
    prods = prods.value_counts(subset=['advertiser_id', 'product_id']).reset_index()

    # Me quedo solo con el top 20 de los productos
    top = prods.sort_values(['advertiser_id', 'count'],ascending=[True, False]).groupby('advertiser_id').head(20)
    top.to_csv('bucket/top_prod.csv', index=False)
    return

def load_sql():
    #Leo las tablas de top product y top ctr 
    prods = pd.read_csv('/home/gbalerdi/progra_avanzada/bucket/top_prod.csv')
    ctr = pd.read_csv('/home/gbalerdi/progra_avanzada/bucket/top_ctr.csv')

    #Genero una columna con un cumulative count de cada advertiser para generar el nombrede las columnas del pivot
    prods['rank'] = (prods.groupby('advertiser_id').cumcount()) + 1
    ctr['rank'] = (ctr.groupby('advertiser_id').cumcount()) + 1

    #Pivoteo las tablas con advertiser_id como index
    prods_table = prods.pivot(index='advertiser_id', columns='rank' ,values='product_id').reset_index()
    ctr_table = ctr.pivot(index='advertiser_id', columns='rank' , values='product_id').reset_index()

    prods_table.to_csv('/home/gbalerdi/progra_avanzada/bucket/prods_table.csv', index=False)
    ctr_table.to_csv('/home/gbalerdi/progra_avanzada/bucket/ctr_table.csv', index= False)

    return None 


active_data()
top_ctr()
top_prod()
load_sql()