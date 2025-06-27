import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def extract_cartpanda_items(data, *args, **kwargs):
    all_orders = data  # mesmo input da API

    line_items_data = []

    for order in all_orders:
        order_id = order.get("id")
        for item in order.get("line_items", []):
            line_items_data.append({
                "order_id": order_id,
                "product_name": item.get("name"),
                "title": item.get("title"),
                "price": item.get("price"),
                "quantity": item.get("quantity"),
                "sku": item.get("sku"),
                "vendor": item.get("vendor"),
                "currency_symbol": item.get("currency_symbol"),
                "total_price": item.get("total_price"),
                "product_main_image": item.get("product_main_image")
            })

    df_items = pd.DataFrame(line_items_data)
    return df_items