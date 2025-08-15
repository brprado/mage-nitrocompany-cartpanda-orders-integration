import pandas as pd
from datetime import datetime
import pytz

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform_cartpanda_customers_data(data, *args, **kwargs):
    all_customers = data

    # Campos principais da tabela de clientes
    selected_fields = [
        "id", "email", "first_name", "last_name", "shop_id", "shop_slug", 
        "created_at", "default_address.country", "default_address.city", 
        "default_address.zip", "default_address.province"
    ]

    # Cria DataFrame com clientes
    df_customers = pd.json_normalize(all_customers, sep='.')

    # Garante que a coluna shop_slug esteja presente
    if 'shop_slug' not in df_customers.columns:
        df_customers['shop_slug'] = [c.get('shop_slug') for c in all_customers]

    # Filtra apenas os campos desejados
    df_customers_filtered = df_customers[[col for col in selected_fields if col in df_customers.columns]]
    
    # Renomeia as colunas do endereço para ficar mais limpo
    df_customers_filtered = df_customers_filtered.rename(columns={
        'default_address.country': 'country',
        'default_address.city': 'city',
        'default_address.zip': 'zip',
        'default_address.province': 'province'
    })
    
    # Dropa IDs nulos
    df_customers_filtered = df_customers_filtered.dropna(subset=['id'])

    # Cria uma coluna para pegar a última data de atualização
    saopaulo_tz = pytz.timezone('America/Sao_Paulo')
    df_customers_filtered['ultima_atualizacao'] = datetime.now(saopaulo_tz)
    
    # Remove duplicados por ID
    df_customers_filtered = df_customers_filtered.drop_duplicates(subset=['id']) 

    # Extração dos endereços por cliente (caso queira uma tabela separada)
    addresses_data = []

    for customer in all_customers:
        customer_id = customer.get("id")
        slug = customer.get("shop_slug")

        for address in customer.get("address", []):
            addresses_data.append({
                "customer_id": customer_id,
                "address_id": address.get("id"),
                "first_name": address.get("first_name"),
                "last_name": address.get("last_name"),
                "company": address.get("company"),
                "address1": address.get("address1"),
                "address2": address.get("address2"),
                "city": address.get("city"),
                "province": address.get("province"),
                "country": address.get("country"),
                "zip": address.get("zip"),
                "phone": address.get("phone"),
                "province_code": address.get("province_code"),
                "country_code": address.get("country_code"),
                "default": address.get("default"),
                "shop_slug": slug
            })

    df_addresses = pd.DataFrame(addresses_data)

    print(f"📊 Total de clientes processados: {len(df_customers_filtered)}")
    print(f"📍 Total de endereços processados: {len(df_addresses)}")

    return {
        "customers_df": df_customers_filtered,
        "addresses_df": df_addresses
    }