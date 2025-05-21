import os
import pandas as pd
from google.cloud import storage
import io
import psycopg2
from datetime import datetime, timedelta 

PROJECT_ID = os.environ.get("AIRFLOW_VAR_PROJECT_ID_GCP", "tpprograavanzanda_default")
RAW_DATA_BUCKET = os.environ.get("AIRFLOW_VAR_RAW_DATA_BUCKET_NAME", f"{PROJECT_ID}-adtech-raw-data")
INTERMEDIATE_DATA_BUCKET = os.environ.get("AIRFLOW_VAR_INTERMEDIATE_DATA_BUCKET_NAME", f"{PROJECT_ID}-adtech-intermediate-data")

CLOUD_SQL_CONNECTION_NAME = os.environ.get("AIRFLOW_VAR_CLOUD_SQL_CONNECTION_NAME", "tu_default_conn_name")
DB_USER = os.environ.get("AIRFLOW_VAR_DB_USER_APP", "postgres")
DB_PASSWORD = os.environ.get("AIRFLOW_VAR_DB_PASSWORD_APP")
DB_NAME_APP = os.environ.get("AIRFLOW_VAR_DB_NAME_APP", "adtech_recommendations")

DB_HOST_FOR_PROXY_SOCKET_DIR = f"/cloudsql/{CLOUD_SQL_CONNECTION_NAME}"

def read_csv_from_gcs(bucket_name, blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        print(f"Intentando leer gs://{bucket_name}/{blob_name}")
        data_string = blob.download_as_text()
        df = pd.read_csv(io.StringIO(data_string))
        print(f"Leído {blob_name} de {bucket_name} exitosamente. Filas: {len(df)}")
        return df
    except Exception as e:
        print(f"Error leyendo {blob_name} de {bucket_name}: {e}")
        raise

def write_df_to_gcs(df, bucket_name, blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        print(f"Intentando escribir gs://{bucket_name}/{blob_name}. Filas: {len(df)}")
        blob.upload_from_string(df.to_csv(index=False), 'text/csv')
        print(f"Escrito {blob_name} a {bucket_name} exitosamente.")
    except Exception as e:
        print(f"Error escribiendo {blob_name} a {bucket_name}: {e}")
        raise

def filtrar_datos_task_callable(**kwargs):
    execution_date_str = kwargs['ds']
    execution_date = datetime.strptime(execution_date_str, '%Y-%m-%d').date()
    print(f"Filtrando datos para la fecha de ejecución: {execution_date_str}")

    advertisers = read_csv_from_gcs(RAW_DATA_BUCKET, 'advertiser_ids.csv')
    ads = read_csv_from_gcs(RAW_DATA_BUCKET, 'ads_views.csv')
    products = read_csv_from_gcs(RAW_DATA_BUCKET, 'product_views.csv')

    active_ads = ads.merge(right=advertisers, how='inner', on='advertiser_id')
    active_products = products.merge(right=advertisers, how='inner', on='advertiser_id')

    active_ads['date'] = pd.to_datetime(active_ads['date']).dt.date
    active_products['date'] = pd.to_datetime(active_products['date']).dt.date

    daily_active_ads = active_ads[active_ads['date'] == execution_date]
    daily_active_products = active_products[active_products['date'] == execution_date]
    
    print(f"Anuncios activos para {execution_date_str}: {len(daily_active_ads)}")
    print(f"Vistas de producto activas para {execution_date_str}: {len(daily_active_products)}")

    if daily_active_ads.empty:
        print(f"ADVERTENCIA: No se encontraron datos de anuncios activos para la fecha {execution_date_str}.")
    if daily_active_products.empty:
        print(f"ADVERTENCIA: No se encontraron datos de productos activos para la fecha {execution_date_str}.")
        
    write_df_to_gcs(daily_active_ads, INTERMEDIATE_DATA_BUCKET, f'active_ads_{execution_date_str}.csv')
    write_df_to_gcs(daily_active_products, INTERMEDIATE_DATA_BUCKET, f'active_products_{execution_date_str}.csv')
    print("Tarea FiltrarDatos completada.")


def top_ctr_task_callable(**kwargs):
    execution_date_str = kwargs['ds']
    print(f"Calculando TopCTR para la fecha de ejecución: {execution_date_str}")
    
    try:
        ads_df = read_csv_from_gcs(INTERMEDIATE_DATA_BUCKET, f'active_ads_{execution_date_str}.csv')
    except Exception as e: 
        print(f"No se pudo leer active_ads_{execution_date_str}.csv. Asumiendo que no hay datos para TopCTR. Error: {e}")
        empty_df = pd.DataFrame(columns=['advertiser_id', 'product_id', 'ctr'])
        write_df_to_gcs(empty_df, INTERMEDIATE_DATA_BUCKET, f'top_ctr_{execution_date_str}.csv')
        print("Tarea TopCTR completada (sin datos de entrada).")
        return

    if ads_df.empty:
        print(f"El archivo active_ads_{execution_date_str}.csv está vacío. No se generará TopCTR.")
        empty_df = pd.DataFrame(columns=['advertiser_id', 'product_id', 'ctr'])
        write_df_to_gcs(empty_df, INTERMEDIATE_DATA_BUCKET, f'top_ctr_{execution_date_str}.csv')
        print("Tarea TopCTR completada (sin datos de entrada).")
        return

    clicks_df = ads_df[ads_df['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='num_clicks')
    impressions_df = ads_df[ads_df['type'] == 'impression'].groupby(['advertiser_id', 'product_id']).size().reset_index(name='num_impressions')
    
    ctr_data = pd.merge(impressions_df, clicks_df, on=['advertiser_id', 'product_id'], how='left')
    ctr_data['num_clicks'] = ctr_data['num_clicks'].fillna(0).astype(int)
    ctr_data['num_impressions'] = ctr_data['num_impressions'].astype(int) # Asegurar que sea int

    ctr_data['ctr'] = ctr_data.apply(
        lambda row: row['num_clicks'] / row['num_impressions'] if row['num_impressions'] > 0 else 0.0, 
        axis=1
    )
    
    top_ctr_df = ctr_data.sort_values(['advertiser_id', 'ctr'], ascending=[True, False]).groupby('advertiser_id').head(20)
    
    write_df_to_gcs(top_ctr_df[['advertiser_id', 'product_id', 'ctr', 'num_clicks', 'num_impressions']], INTERMEDIATE_DATA_BUCKET, f'top_ctr_{execution_date_str}.csv')
    print("Tarea TopCTR completada.")


def top_product_task_callable(**kwargs):
    execution_date_str = kwargs['ds']
    print(f"Calculando TopProduct para la fecha de ejecución: {execution_date_str}")

    try:
        products_df = read_csv_from_gcs(INTERMEDIATE_DATA_BUCKET, f'active_products_{execution_date_str}.csv')
    except Exception as e:
        print(f"No se pudo leer active_products_{execution_date_str}.csv. Asumiendo que no hay datos para TopProduct. Error: {e}")
        empty_df = pd.DataFrame(columns=['advertiser_id', 'product_id', 'view_count'])
        write_df_to_gcs(empty_df, INTERMEDIATE_DATA_BUCKET, f'top_product_{execution_date_str}.csv')
        print("Tarea TopProduct completada (sin datos de entrada).")
        return

    if products_df.empty:
        print(f"El archivo active_products_{execution_date_str}.csv está vacío. No se generará TopProduct.")
        empty_df = pd.DataFrame(columns=['advertiser_id', 'product_id', 'view_count'])
        write_df_to_gcs(empty_df, INTERMEDIATE_DATA_BUCKET, f'top_product_{execution_date_str}.csv')
        print("Tarea TopProduct completada (sin datos de entrada).")
        return

    product_counts_df = products_df.groupby(['advertiser_id', 'product_id']).size().reset_index(name='view_count')
    
    top_products_df = product_counts_df.sort_values(['advertiser_id', 'view_count'], ascending=[True, False]).groupby('advertiser_id').head(20)
    
    write_df_to_gcs(top_products_df[['advertiser_id', 'product_id', 'view_count']], INTERMEDIATE_DATA_BUCKET, f'top_product_{execution_date_str}.csv')
    print("Tarea TopProduct completada.")


def db_writing_task_callable(**kwargs):
    execution_date_str = kwargs['ds']
    print(f"Escribiendo recomendaciones a DB para la fecha: {execution_date_str}")

    try:
        top_ctr_recs_df = read_csv_from_gcs(INTERMEDIATE_DATA_BUCKET, f'top_ctr_{execution_date_str}.csv')
    except Exception as e:
        print(f"No se pudo leer top_ctr_{execution_date_str}.csv. Saltando escritura para TopCTR. Error: {e}")
        top_ctr_recs_df = pd.DataFrame() # Vacío para que no falle el resto

    try:
        top_product_recs_df = read_csv_from_gcs(INTERMEDIATE_DATA_BUCKET, f'top_product_{execution_date_str}.csv')
    except Exception as e:
        print(f"No se pudo leer top_product_{execution_date_str}.csv. Saltando escritura para TopProduct. Error: {e}")
        top_product_recs_df = pd.DataFrame() # Vacío para que no falle el resto


    conn = None
    try:
        conn_params = {
            "host": f"/cloudsql/{CLOUD_SQL_CONNECTION_NAME}",
            "database": DB_NAME_APP,
            "user": DB_USER,
            "password": DB_PASSWORD
        }
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        print(f"Conectado a PostgreSQL para escribir recomendaciones de fecha {execution_date_str}")

        if not top_ctr_recs_df.empty:
            print(f"Procesando {len(top_ctr_recs_df)} recomendaciones de TopCTR.")
            cur.execute("DELETE FROM recommendations WHERE generated_at = %s AND model_type = %s", (execution_date_str, 'TopCTR'))
            
            if 'rank' not in top_ctr_recs_df.columns:
                 top_ctr_recs_df['rank'] = top_ctr_recs_df.groupby('advertiser_id').cumcount() + 1
            
            for index, row in top_ctr_recs_df.iterrows():
                cur.execute(
                    "INSERT INTO recommendations (advertiser_id, model_type, product_id, recommendation_rank, generated_at) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (row['advertiser_id'], 'TopCTR', row['product_id'], int(row['rank']), execution_date_str)
                )
            print("Recomendaciones de TopCTR escritas.")
        else:
            print("No hay datos de TopCTR para escribir en la BD.")

        if not top_product_recs_df.empty:
            print(f"Procesando {len(top_product_recs_df)} recomendaciones de TopProduct.")
            cur.execute("DELETE FROM recommendations WHERE generated_at = %s AND model_type = %s", (execution_date_str, 'TopProduct'))
            
            if 'rank' not in top_product_recs_df.columns:
                 top_product_recs_df['rank'] = top_product_recs_df.groupby('advertiser_id').cumcount() + 1

            for index, row in top_product_recs_df.iterrows():
                cur.execute(
                    "INSERT INTO recommendations (advertiser_id, model_type, product_id, recommendation_rank, generated_at) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (row['advertiser_id'], 'TopProduct', row['product_id'], int(row['rank']), execution_date_str)
                )
            print("Recomendaciones de TopProduct escritas.")
        else:
            print("No hay datos de TopProduct para escribir en la BD.")

        conn.commit()
        print("Datos escritos a PostgreSQL exitosamente.")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error escribiendo a PostgreSQL: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Conexión PostgreSQL cerrada.")
    print("Tarea DBWriting completada.")