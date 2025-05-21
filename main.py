from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field 
import psycopg2
from psycopg2.extras import RealDictCursor 
import os
from dotenv import load_dotenv 
from typing import List, Dict, Any, Set 
from datetime import date, timedelta

load_dotenv()

app = FastAPI(
    title="AdTech Recommendation API",
    description="API para dar recomendaciones de productos generadas por el pipeline de AdTech.",
    version="1.0.0"
)

def get_db_connection_params():
    if os.environ.get("K_SERVICE"):
        cloud_sql_connection_name = os.environ.get('CLOUD_SQL_CONNECTION_NAME')
        print(f"DEBUG CLOUD RUN: CLOUD_SQL_CONNECTION_NAME leído como: {cloud_sql_connection_name}")
        db_name = os.environ.get('DB_NAME')
        print(f"DEBUG CLOUD RUN: DB_NAME leído como: {db_name}")
        db_user = os.environ.get('DB_USER')
        print(f"DEBUG CLOUD RUN: DB_USER leído como: {db_user}")
        db_password = os.environ.get('DB_PASSWORD')
        print(f"DEBUG CLOUD RUN: DB_PASSWORD leído como: {db_password}")

        if not all([cloud_sql_connection_name, db_name, db_user, db_password]):
            print("ERROR: Faltan variables de entorno para la conexión a Cloud SQL en Cloud Run.")

        return {
            "host": f"/cloudsql/{cloud_sql_connection_name}", 
            "database": db_name,
            "user": db_user,
            "password": db_password
        }
    else: 
        return {
            "host": os.environ.get("DB_HOST"),
            "port": os.environ.get("DB_PORT", 5432),
            "database": os.environ.get("DB_NAME"),
            "user": os.environ.get("DB_USER"),
            "password": os.environ.get("DB_PASSWORD")
        }

def get_db_connection():
    conn_params = get_db_connection_params()
    try:
        conn = psycopg2.connect(**conn_params)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error crítico conectando a la base de datos: {e}")
        # Este error es crítico para la API, así que levantamos una excepción clara.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
            detail=f"No se pudo conectar a la base de datos. Error: {e}"
        )
    
class RecommendationItem(BaseModel):
    product_id: str
    recommendation_rank: int

class AdvertiserRecommendations(BaseModel):
    advertiser_id: str
    model_type: str
    generated_at: date
    recommendations: List[RecommendationItem]

class HistoryRecommendations(BaseModel):
    advertiser_id: str
    recommendations_by_date: Dict[date, Dict[str, List[RecommendationItem]]]

class AdvertiserVariation(BaseModel):
    advertiser_id: str
    model_type: str
    date1: date
    date2: date
    products_date1_count: int
    products_date2_count: int
    common_products_count: int
    variation_score: float = Field(..., description="Ej: 1.0 - (common / max(count_d1, count_d2)). 0=sin variación, 1=totalmente diferente.")

class ModelCoincidence(BaseModel):
    advertiser_id: str
    date: date
    top_ctr_products_count: int
    top_product_products_count: int
    coincident_products_count: int
    coincidence_percentage: float = Field(..., description="Ej: (coincident / min_count_entre_modelos_no_cero) * 100")

class StatsSummary(BaseModel):
    latest_recommendation_date: date | None = None
    total_advertisers_with_recommendations_today: int 
    advertisers_ranked_by_variation: List[AdvertiserVariation] 
    model_coincidence_stats: List[ModelCoincidence]


# --- Endpoints de la API ---
@app.get("/")
async def root():
    return {"message": "Bienvenido a la API de Recomendaciones AdTech"}

@app.get("/recommendations/{advertiser_id}/{model_type}", response_model=AdvertiserRecommendations)
async def get_recommendations(advertiser_id: str, model_type: str):
    if model_type not in ["TopCTR", "TopProduct"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Modelo inválido. Usar 'TopCTR' o 'TopProduct'.")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT MAX(generated_at) as latest_date FROM recommendations WHERE advertiser_id = %s AND model_type = %s",
                (advertiser_id, model_type)
            )
            latest_date_row = cur.fetchone()
            if not latest_date_row or not latest_date_row['latest_date']:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No hay recomendaciones para {advertiser_id} con modelo {model_type}")
            
            latest_generated_date = latest_date_row['latest_date']

            cur.execute(
                "SELECT product_id, recommendation_rank FROM recommendations WHERE advertiser_id = %s AND model_type = %s AND generated_at = %s ORDER BY recommendation_rank ASC",
                (advertiser_id, model_type, latest_generated_date)
            )
            recs_data = cur.fetchall()
            
        return AdvertiserRecommendations(
            advertiser_id=advertiser_id,
            model_type=model_type,
            generated_at=latest_generated_date,
            recommendations=[RecommendationItem(**row) for row in recs_data]
        )
    except HTTPException: 
        raise
    except psycopg2.Error as e:
        print(f"Error de base de datos en /recommendations: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor al consultar la base de datos.")
    except Exception as e:
        print(f"Error inesperado en /recommendations: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno inesperado del servidor.")
    finally:
        if conn:
            conn.close()

@app.get("/history/{advertiser_id}", response_model=HistoryRecommendations)
async def get_history(advertiser_id: str):
    end_date = date.today()
    start_date = end_date - timedelta(days=6) 
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT product_id, recommendation_rank, model_type, generated_at FROM recommendations WHERE advertiser_id = %s AND generated_at BETWEEN %s AND %s ORDER BY generated_at DESC, model_type, recommendation_rank ASC",
                (advertiser_id, start_date, end_date)
            )
            history_data = cur.fetchall()
        
        if not history_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No hay historial de recomendaciones para {advertiser_id} en los últimos 7 días")

        recommendations_by_date_and_model: Dict[date, Dict[str, List[RecommendationItem]]] = {}
        for row in history_data:
            gen_date = row['generated_at']
            model = row['model_type']
            item = RecommendationItem(product_id=row['product_id'], recommendation_rank=row['recommendation_rank'])
            
            recommendations_by_date_and_model.setdefault(gen_date, {}).setdefault(model, []).append(item)

        return HistoryRecommendations(
            advertiser_id=advertiser_id,
            recommendations_by_date=recommendations_by_date_and_model
        )
    except HTTPException:
        raise
    except psycopg2.Error as e:
        print(f"Error de base de datos en /history: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor al consultar la base de datos.")
    except Exception as e:
        print(f"Error inesperado en /history: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno inesperado del servidor.")
    finally:
        if conn:
            conn.close()

@app.get("/stats/", response_model=StatsSummary) 
async def get_stats():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            
            cur.execute("SELECT MAX(generated_at) as latest_date FROM recommendations")
            latest_date_row = cur.fetchone()
            if not latest_date_row or not latest_date_row['latest_date']:
                return StatsSummary( # Devolver stats vacías si no hay datos
                    latest_recommendation_date=None,
                    total_advertisers_with_recommendations_today=0,
                    advertisers_ranked_by_variation=[],
                    model_coincidence_stats=[]
                )
            latest_date = latest_date_row['latest_date']

            cur.execute(
                "SELECT COUNT(DISTINCT advertiser_id) as count FROM recommendations WHERE generated_at = %s",
                (latest_date,)
            )
            count_row = cur.fetchone()
            total_advertisers_latest_date = count_row['count'] if count_row else 0

            advertiser_variations_list: List[AdvertiserVariation] = []
            cur.execute(
                "SELECT generated_at FROM recommendations WHERE generated_at < %s GROUP BY generated_at ORDER BY generated_at DESC LIMIT 1",
                (latest_date,)
            )
            prev_date_row = cur.fetchone()
            previous_date = prev_date_row['generated_at'] if prev_date_row else None

            if previous_date:
                cur.execute(
                    "SELECT advertiser_id, model_type, product_id, generated_at FROM recommendations WHERE generated_at IN (%s, %s) ORDER BY advertiser_id, model_type, generated_at",
                    (latest_date, previous_date)
                )
                all_recs_for_variation = cur.fetchall()
                
                recs_by_adv_model_date: Dict[tuple[str, str], Dict[date, Set[str]]] = {}
                for row in all_recs_for_variation:
                    key = (row['advertiser_id'], row['model_type'])
                    current_date = row['generated_at']
                    product = row['product_id']
                    recs_by_adv_model_date.setdefault(key, {}).setdefault(current_date, set()).add(product)

                for (adv_id, model), date_recs in recs_by_adv_model_date.items():
                    recs_latest = date_recs.get(latest_date, set())
                    recs_previous = date_recs.get(previous_date, set())
                    
                    common_products_count = len(recs_latest.intersection(recs_previous))
                    denominator = max(len(recs_latest), len(recs_previous))
                    variation_score = 0.0
                    if denominator > 0:
                        variation_score = 1.0 - (common_products_count / denominator)
                    
                    advertiser_variations_list.append(AdvertiserVariation(
                        advertiser_id=adv_id, model_type=model, date1=previous_date, date2=latest_date,
                        products_date1_count=len(recs_previous), products_date2_count=len(recs_latest),
                        common_products_count=common_products_count, variation_score=round(variation_score, 4)
                    ))
                advertiser_variations_list.sort(key=lambda x: x.variation_score, reverse=True)

            model_coincidence_list: List[ModelCoincidence] = []
            cur.execute(
                "SELECT advertiser_id, model_type, product_id FROM recommendations WHERE generated_at = %s ORDER BY advertiser_id, model_type",
                (latest_date,)
            )
            recs_latest_date_all_models = cur.fetchall()

            adv_model_products: Dict[str, Dict[str, Set[str]]] = {}
            for row in recs_latest_date_all_models:
                adv_id, model, product = row['advertiser_id'], row['model_type'], row['product_id']
                adv_model_products.setdefault(adv_id, {}).setdefault(model, set()).add(product)
            
            for adv_id, models_data in adv_model_products.items():
                top_ctr_set = models_data.get("TopCTR", set())
                top_product_set = models_data.get("TopProduct", set())
                
                coincident_products_count = len(top_ctr_set.intersection(top_product_set))
                
                count_ctr = len(top_ctr_set)
                count_prod = len(top_product_set)
                coincidence_percentage = 0.0
                
                if count_ctr > 0 and count_prod > 0:
                    denominator_coincidence = min(count_ctr, count_prod)
                    coincidence_percentage = (coincident_products_count / denominator_coincidence) * 100.0
                elif count_ctr > 0 or count_prod > 0: 
                    coincidence_percentage = 0.0 

                model_coincidence_list.append(ModelCoincidence(
                    advertiser_id=adv_id, date=latest_date,
                    top_ctr_products_count=count_ctr, top_product_products_count=count_prod,
                    coincident_products_count=coincident_products_count,
                    coincidence_percentage=round(coincidence_percentage, 2)
                ))
            model_coincidence_list.sort(key=lambda x: x.advertiser_id)


            return StatsSummary(
                latest_recommendation_date=latest_date,
                total_advertisers_with_recommendations_today=total_advertisers_latest_date,
                advertisers_ranked_by_variation=advertiser_variations_list,
                model_coincidence_stats=model_coincidence_list
            )

    except HTTPException:
        raise
    except psycopg2.Error as e:
        print(f"Error de base de datos en /stats: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno del servidor al consultar la base de datos.")
    except Exception as e:
        print(f"Error inesperado en /stats: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno inesperado del servidor.")
    finally:
        if conn:
            conn.close()