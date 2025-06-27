from sqlalchemy import create_engine
from mage_ai.data_preparation.shared.secrets import get_secret_value
import pandas as pd
import json

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exporta os dados do bloco anterior para um banco PostgreSQL usando segredos do Mage.
    Serializa colunas do tipo dict ou list como strings JSON.
    """

    # Captura das variáveis seguras definidas nos Secrets
    POSTGRES_HOST = get_secret_value('POSTGRES_HOST')
    POSTGRES_PORT = get_secret_value('DB_PORT') 
    POSTGRES_DB   = get_secret_value('DB_NAME') 
    POSTGRES_USER = get_secret_value('DB_USER') 
    POSTGRES_PASS = get_secret_value('DB_PASSWORD') 

    TABLE_NAME = 'tickets_movidesk'

    # Constrói a connection string
    connection_string = (
        f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}'
        f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    )
    engine = create_engine(connection_string)

    # 🔧 Converte colunas com dict ou list para JSON string
    for col in data.columns:
        if data[col].apply(lambda x: isinstance(x, (dict, list))).any():
            data[col] = data[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    # Exporta para PostgreSQL
    data.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)

    print(f"✅ Dados exportados com sucesso para a tabela '{TABLE_NAME}'.")
    return f"{len(data)} registros exportados para a tabela '{TABLE_NAME}'."