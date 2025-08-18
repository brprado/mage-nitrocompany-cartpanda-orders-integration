import requests

from concurrent.futures import ThreadPoolExecutor, as_completed

from time import sleep

import requests

from mage_ai.data_preparation.shared.secrets import get_secret_value



if 'data_loader' not in globals():

    from mage_ai.data_preparation.decorators import data_loader



# Concurrent API Requests (Paralelismo) 

def fetch_orders_for_slug(slug, headers):

    API_URL = f'https://accounts.cartpanda.com/api/v3/{slug}/orders'

    page = 1

    all_orders = []



    while True:

        params = {'page': page, 'limit': 200}

        response = requests.get(API_URL, headers=headers, params=params)

        response.raise_for_status()



        data = response.json()

        orders = data.get('orders', [])

        meta = data.get('meta', {})



        print(f"→ {slug} | Página {page}: {len(orders)} pedidos")



        for order in orders:

            order['shop_slug'] = slug



        all_orders.extend(orders)



        if meta.get('current_page', page) >= meta.get('last_page', page):

            break

        page += 1

        sleep(1)



    return all_orders



@data_loader

def cartpanda_orders_extraction(*args, **kwargs):

    slugs = ['vita-waves', 'nutra-force-wl', 'nutra-force-di', 'nutra-force','vita-labs']

    API_KEY = get_secret_value('CARTPANDA_API_KEY')

    headers = {

        'Authorization': f'Bearer {API_KEY}',

        'Accept': 'application/json',

    }



    all_orders = []



    with ThreadPoolExecutor(max_workers=len(slugs)) as executor:

        future_to_slug = {executor.submit(fetch_orders_for_slug, slug, headers): slug for slug in slugs}



        for future in as_completed(future_to_slug):

            slug = future_to_slug[future]

            try:

                orders = future.result()

                print(f"✅ Coleta finalizada para: {slug} ({len(orders)} pedidos)")

                all_orders.extend(orders)

            except Exception as e:

                print(f"❌ Erro ao coletar pedidos da loja {slug}: {e}")



    print(f"\n📦 Total geral de pedidos coletados: {len(all_orders)}")

    return all_orders
