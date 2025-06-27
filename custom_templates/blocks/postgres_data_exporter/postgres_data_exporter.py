import os
from sqlalchemy import create_engine

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exporta dados para um banco de dados PostgreSQL.
    """

    # Configurações de conexão (recomenda-se usar variáveis de ambiente)
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'seu_usuario')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'sua_senha')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'seu_banco')
    TABLE_NAME = os.getenv('POSTGRES_TABLE', 'tickets_movidesk')

    # Cria a engine de conexão com o PostgreSQL
    connection_string = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    engine = create_engine(connection_string)

    # Exporta para o banco
    data.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)

    print(f"✅ Dados exportados com sucesso para a tabela '{TABLE_NAME}' no PostgreSQL.")
    return f"Exportados {len(data)} registros para a tabela '{TABLE_NAME}'."
