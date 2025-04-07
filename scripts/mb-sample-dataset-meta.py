import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


def random_date(start: str, end:str) -> datetime:
    """
    gENERA UN RANGO DE FECHAS

    Args:
        start (str): FECHA DE INICIO con formato "yyyy-mm-dd"
        end (str): FECHA DE FIN con formato "yyyy-mm-dd"

    Returns:
        datetime: _description_
    """
    time_between_dates = end - start
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start + timedelta(days=random_number_of_days)
    return random_date


if __name__ == '__main__':

    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 12, 31)
    num_campanas = 5
    num_grupos_anuncios = 15
    num_anuncios = 50
    date_range = pd.date_range(start_date, end_date)

    # Campañas
    marcas = ['Marca A', 'Marca B', 'Marca C']
    tipos_campana = ['Promoción', 'Conciencia', 'Lanzamiento']
    campanas_data = {
        'ID_Campaña': range(1001, 1001 + num_campanas),
        'Nombre_Campaña': [f'Campaña-{i}-{random.choice(marcas)}-{random.choice(tipos_campana)}' for i in range(1, num_campanas + 1)]
    }
    df_campanas = pd.DataFrame(campanas_data)
    df_campanas.to_csv('campanas.csv', index=False)

    # Grupos de anuncios
    grupos_anuncios_data = {
        'ID_Grupo_Anuncios': range(2001, 2001 + num_grupos_anuncios),
        'ID_Campaña': np.random.choice(df_campanas['ID_Campaña'], num_grupos_anuncios),
        'Nombre_Grupo_Anuncios': [f'Grupo-{i}-{np.random.choice(["Mujeres", "Hombres", "Jovenes"])}' for i in range(1, num_grupos_anuncios + 1)]
    }
    df_grupos_anuncios = pd.DataFrame(grupos_anuncios_data)
    df_grupos_anuncios.to_csv('grupos_anuncios.csv', index=False)

    # Anuncios
    anuncios_data = {
        'ID_Anuncio': range(3001, 3001 + num_anuncios),
        'ID_Grupo_Anuncios': np.random.choice(df_grupos_anuncios['ID_Grupo_Anuncios'], num_anuncios),
        'Nombre_Anuncio': [f'Anuncio-{i}-{np.random.choice(["Imagen", "Video", "Carrusel"])}' for i in range(1, num_anuncios + 1)],
        'Plataforma': np.random.choice(['Facebook', 'Instagram', 'Audience Network'], num_anuncios)
    }
    df_anuncios = pd.DataFrame(anuncios_data)
    df_anuncios.to_csv('anuncios.csv', index=False)

    # Datos diarios
    daily_data = []
    for date in date_range:
        for anuncio_id in df_anuncios['ID_Anuncio']:
            impresiones = random.randint(100, 5000)

            # Divisiones aleatorias para clics, interacciones y conversiones
            div_clics = random.randint(2, 10)
            div_interacciones = random.randint(2, 5)
            div_conversiones = random.randint(2, 3)

            clics = random.randint(1, impresiones // div_clics)
            interacciones = random.randint(1, clics * div_interacciones)
            conversiones = random.randint(0, interacciones // div_conversiones)

            # Quartiles basados en impresiones y divisiones aleatorias
            div_q25 = random.randint(2, 10)
            div_q50 = random.randint(2, 10)
            div_q75 = random.randint(2, 10)
            q25 = random.randint(impresiones // div_q25,
                                impresiones // (div_q25 - 1))
            q50 = random.randint(q25 // div_q50, q25 // (div_q50 - 1))
            q75 = random.randint(q50 // div_q75, q50 // (div_q75 - 1))
            completado = random.randint(q75 // 2, q75)

            # Gasto (Spend) relacionado con impresiones y clics
            spend = random.uniform(0.1, 1.0) * (impresiones /
                                                1000) + random.uniform(0.05, 0.5) * clics

            daily_data.append({
                'Fecha': date.strftime('%Y-%m-%d'),
                'ID_Anuncio': anuncio_id,
                'Impresiones': impresiones,
                'Clics': clics,
                'Interacciones': interacciones,
                'Conversiones': conversiones,
                'Quartil_25': q25,
                'Quartil_50': q50,
                'Quartil_75': q75,
                'Completado': completado,
                'Spend': round(spend, 2)
            })
    df_daily = pd.DataFrame(daily_data)
    df_daily.to_csv('daily_data.csv', index=False)

    print("Archivos CSV generados con éxito: campanas.csv, grupos_anuncios.csv, anuncios.csv, daily_data.csv")
