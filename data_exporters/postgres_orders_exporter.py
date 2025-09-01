from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def sanitize_for_postgres(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)
    return df

def add_primary_keys(engine, schema_name):
    """Adiciona chaves primárias nas tabelas após a criação"""
    with engine.begin() as conn:
        # Adiciona chave primária na tabela orders (coluna id)
        try:
            conn.execute(text(f"""
                ALTER TABLE {schema_name}.cartpanda_orders 
                ADD CONSTRAINT pk_cartpanda_orders PRIMARY KEY (id)
            """))
            print("Chave primária 'id' adicionada na tabela cartpanda_orders")
        except Exception as e:
            print(f"Erro ao adicionar PK na tabela orders: {e}")
        
        # Adiciona chave primária na tabela items (coluna item_id)
        try:
            conn.execute(text(f"""
                ALTER TABLE {schema_name}.cartpanda_items 
                ADD CONSTRAINT pk_cartpanda_items PRIMARY KEY (item_id)
            """))
            print("Chave primária 'item_id' adicionada na tabela cartpanda_items")
        except Exception as e:
            print(f"Erro ao adicionar PK na tabela items: {e}")

@data_exporter
def export_cartpanda_data(data, *args, **kwargs):
    df_orders = sanitize_for_postgres(data['orders_df'])
    df_items = sanitize_for_postgres(data['items_df'])

    # POSTGRES_HOST = get_secret_value('POSTGRES_HOST')
    # POSTGRES_PORT = get_secret_value('DB_PORT')
    # POSTGRES_DB   = get_secret_value('DB_NAME')
    # POSTGRES_USER = get_secret_value('DB_USER')
    # POSTGRES_PASS = get_secret_value('DB_PASSWORD')

    ########### Exportar dados Datalake VPS ###########
    # connection_string = (
    #     f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}'
    #     f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    # )

    # engine_rep = create_engine(connection_string)

    # # Garante a existência do schema "integracao"
    # with engine_rep.begin() as conn:
    #     conn.execute(text("CREATE SCHEMA IF NOT EXISTS integracao"))

    # Exporta orders
    # df_orders.to_sql(
    #     schema='integracao',
    #     name='cartpanda_orders',
    #     con=engine_rep,
    #     if_exists='replace',
    #     index=False
    # )

    # # Exporta itens
    # df_items.to_sql(
    #     schema='integracao',
    #     name='cartpanda_items',
    #     con=engine_rep,
    #     if_exists='replace',
    #     index=False
    # )

    # # Adiciona as chaves primárias
    # add_primary_keys(engine_rep, 'integracao')
    # print('Dados Exportados Para Replicacao/Dev (PostgreSQL VPS HOSTINGER)')

    ### Exportar para DataLake Railway
    POSTGRES_HOST = get_secret_value('POSTGRES_HOST_RAILWAY')
    POSTGRES_PORT = get_secret_value('POSTGRES_PORT_RAILWAY')
    POSTGRES_DB   = get_secret_value('POSTGRES_DB_RAILWAY')
    POSTGRES_USER = get_secret_value('POSTGRES_USER_RAILWAY')
    POSTGRES_PASS = get_secret_value('POSTGRES_PASS_RAILWAY')

    ########## Exportar dados Datalake VPS ###########
    datalake_railway_conn_string = (
        f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}'
        f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    )
    engine_railway = create_engine(datalake_railway_conn_string)
    
    with engine_railway.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS integracao"))
    
    # Exporta orders
    df_orders.to_sql(
        schema='integracao',
        name='cartpanda_orders',
        con=engine_railway,
        if_exists='replace',
        index=False
    )

    # Exporta itens
    df_items.to_sql(
        schema='integracao',
        name='cartpanda_items',
        con=engine_railway,
        if_exists='replace',
        index=False
    )

    # Adiciona as chaves primárias no Railway também
    add_primary_keys(engine_railway, 'integracao')
    print('Dados Exportados Para DataLake Railway (PostgreSQL)')