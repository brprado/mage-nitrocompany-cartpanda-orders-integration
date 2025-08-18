from mage_ai.data_preparation.shared.secrets import get_secret_value
from sqlalchemy import create_engine, text
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def sanitize_for_postgres(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (dict, list)) else x)
    return df

def upsert_dataframe(df, table_name, schema, engine, primary_key):
    """
    Função para fazer upsert (INSERT ... ON CONFLICT DO UPDATE) no PostgreSQL
    """
    temp_table = f"{table_name}_temp"
    
    with engine.begin() as conn:
        # 1. Criar tabela temporária com os novos dados
        df.to_sql(
            name=temp_table,
            schema=schema,
            con=conn,
            if_exists='replace',
            index=False
        )
        
        # 2. Verificar se a tabela principal existe
        table_exists = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table_name}'
            )
        """)).scalar()
        
        if not table_exists:
            # 3a. Criar tabela principal pela primeira vez (com constraint)
            columns = df.columns.tolist()
            
            # Definir tipos de colunas baseado no DataFrame
            column_definitions = []
            for col in columns:
                if col == primary_key:
                    if df[col].dtype == 'int64':
                        column_definitions.append(f'"{col}" BIGINT PRIMARY KEY')
                    else:
                        column_definitions.append(f'"{col}" TEXT PRIMARY KEY')
                else:
                    if df[col].dtype == 'int64':
                        column_definitions.append(f'"{col}" BIGINT')
                    elif df[col].dtype == 'float64':
                        column_definitions.append(f'"{col}" DOUBLE PRECISION')
                    elif df[col].dtype == 'bool':
                        column_definitions.append(f'"{col}" BOOLEAN')
                    else:
                        column_definitions.append(f'"{col}" TEXT')
            
            create_table_sql = f"""
                CREATE TABLE {schema}.{table_name} (
                    {', '.join(column_definitions)}
                )
            """
            conn.execute(text(create_table_sql))
            print(f"📝 Tabela {table_name} criada com PRIMARY KEY em '{primary_key}'")
        else:
            # 3b. Verificar se existe uma constraint de unicidade na coluna desejada
            constraint_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = '{schema}' 
                    AND tc.table_name = '{table_name}'
                    AND kcu.column_name = '{primary_key}'
                    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                )
            """)).scalar()
            
            if not constraint_exists:
                try:
                    # Verificar se já existe uma PRIMARY KEY na tabela
                    pk_exists = conn.execute(text(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.table_constraints 
                            WHERE table_schema = '{schema}' 
                            AND table_name = '{table_name}'
                            AND constraint_type = 'PRIMARY KEY'
                        )
                    """)).scalar()
                    
                    if pk_exists:
                        # Já existe PK, criar UNIQUE constraint
                        conn.execute(text(f"""
                            ALTER TABLE {schema}.{table_name} 
                            ADD CONSTRAINT uk_{table_name}_{primary_key} 
                            UNIQUE ("{primary_key}")
                        """))
                        print(f"🔑 UNIQUE constraint adicionada na coluna '{primary_key}' (PRIMARY KEY já existe)")
                    else:
                        # Não existe PK, pode criar uma
                        conn.execute(text(f"""
                            ALTER TABLE {schema}.{table_name} 
                            ADD PRIMARY KEY ("{primary_key}")
                        """))
                        print(f"🔑 PRIMARY KEY adicionada na coluna '{primary_key}'")
                        
                except Exception as e:
                    print(f"⚠️ Não foi possível adicionar constraint: {e}")
                    # Como última alternativa, tentar UNIQUE com nome diferente
                    try:
                        conn.execute(text(f"""
                            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_{table_name}_{primary_key}
                            ON {schema}.{table_name} ("{primary_key}")
                        """))
                        print(f"🔑 UNIQUE INDEX criado na coluna '{primary_key}'")
                    except Exception as e2:
                        print(f"❌ Erro ao criar qualquer constraint: {e2}")
                        raise
            else:
                print(f"✅ Constraint de unicidade já existe na coluna '{primary_key}'")
        
        # 4. Preparar colunas para o upsert
        columns = df.columns.tolist()
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        # Colunas para UPDATE (excluindo a chave primária)
        update_columns = [col for col in columns if col != primary_key]
        update_str = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])
        
        # 5. Executar UPSERT
        upsert_query = f"""
        INSERT INTO {schema}.{table_name} ({columns_str})
        SELECT {columns_str} FROM {schema}.{temp_table}
        ON CONFLICT ("{primary_key}") 
        DO UPDATE SET {update_str}
        """
        
        conn.execute(text(upsert_query))
        
        # 6. Remover tabela temporária
        conn.execute(text(f"DROP TABLE {schema}.{temp_table}"))
        
        print(f"✅ Upsert concluído para {table_name}: {len(df)} registros processados")

@data_exporter
def export_cartpanda_data(data, *args, **kwargs):
    df_orders = sanitize_for_postgres(data['orders_df'])
    df_items = sanitize_for_postgres(data['items_df'])

    POSTGRES_HOST = get_secret_value('POSTGRES_HOST')
    POSTGRES_PORT = get_secret_value('DB_PORT')
    POSTGRES_DB   = get_secret_value('DB_NAME')
    POSTGRES_USER = get_secret_value('DB_USER')
    POSTGRES_PASS = get_secret_value('DB_PASSWORD')

    ########### REPLICATION ###########

    connection_string = (
        f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}'
        f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    )

    engine_rep = create_engine(connection_string)

    # Garante a existência do schema "integracao"
    with engine_rep.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS integracao"))

    # UPSERT para pedidos (chave primária: id)
    upsert_dataframe(
        df=df_orders,
        table_name='cartpanda_orders',
        schema='integracao',
        engine=engine_rep,
        primary_key='id'
    )

    # UPSERT para itens (chave primária: item_id)
    upsert_dataframe(
        df=df_items,
        table_name='cartpanda_items',
        schema='integracao',
        engine=engine_rep,
        primary_key='item_id'
    )
    
    print('✅ Orders: UPSERT (id) | Items: UPSERT (item_id) - Dados Exportados Para Replicacao/Dev (PostgreSQL VPS HOSTINGER)')

