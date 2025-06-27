if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_to_postgres(data, *args, **kwargs):
    # Dados recebidos do bloco anterior
    df_orders = data['orders_df']
    df_items = data['items_df']

    # Recupera secrets do Mage
    POSTGRES_HOST = get_secret_value('POSTGRES_HOST')
    POSTGRES_PORT = get_secret_value('DB_PORT')
    POSTGRES_DB   = get_secret_value('DB_NAME')
    POSTGRES_USER = get_secret_value('DB_USER')
    POSTGRES_PASS = get_secret_value('DB_PASSWORD')

    # Cria connection string
    connection_string = (
        f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}'
        f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    )
    engine = create_engine(connection_string)

    # Exporta pedidos
    df_orders.to_sql(
        name='cartpanda_orders',
        con=engine,
        if_exists='replace',  # ou 'append' se quiser acumular
        index=False
    )

    # Exporta produtos por pedido
    df_items.to_sql(
        name='cartpanda_items',
        con=engine,
        if_exists='replace',  # ou 'append'
        index=False
    )