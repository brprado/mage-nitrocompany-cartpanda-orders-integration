from mage_ai.data_preparation.shared.secrets import get_secret_value
import requests
from time import sleep


@data_loader
def cartpanda_orders_extraction(*args, **kwargs):
    API_URL = 'https://accounts.cartpanda.com/api/v3/nutra-force/orders'
    API_KEY = get_secret_value('CARTPANDA_API_KEY')
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Accept': 'application/json',
    }

    page = 1
    all_orders = []

    while True:
        params = {'page': page, 'limit':250}
        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        orders = data.get('orders', [])
        meta = data.get('meta', {})

        print(f"Página {page}: {len(orders)} pedidos")
        all_orders.extend(orders)

        if meta.get('current_page', page) >= meta.get('last_page', page):
            break

        page += 1
        sleep(1)

    return all_orders