from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def sanitize_for_postgres(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)
    return df

@data_exporter
def export_cartpanda_data(data, *args, **kwargs):
    df_orders = sanitize_for_postgres(data['orders_df'])
    df_items = sanitize_for_postgres(data['items_df'])

    POSTGRES_HOST = get_secret_value('POSTGRES_HOST')
    POSTGRES_PORT = get_secret_value('DB_PORT')
    POSTGRES_DB   = get_secret_value('DB_NAME')
    POSTGRES_USER = get_secret_value('DB_USER')
    POSTGRES_PASS = get_secret_value('DB_PASSWORD')

    connection_string = (
        f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}'
        f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    )
    engine = create_engine(connection_string)

    # Garante a existência do schema "integracao"
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS integracao"))

    # Exporta pedidos
    df_orders.to_sql(
        schema='integracao',
        name='cartpanda_orders',
        con=engine,
        if_exists='replace',
        index=False
    )

    # Exporta itens
    df_items.to_sql(
        schema='integracao',
        name='cartpanda_items',
        con=engine,
        if_exists='replace',
        index=False
    )