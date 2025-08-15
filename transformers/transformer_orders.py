import pandas as pd
from datetime import datetime
import pytz

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform_cartpanda_data(data, *args, **kwargs):
    all_orders = data

    # Campos principais da tabela de pedidos
    selected_fields = [
        "id","status_id", "browser_ip", "buyer_accepts_marketing", "buyer_accepts_phone_marketing",
        "cancel_reason", "cancelled_at", "cart_token", "client_details", "closed_at",
        "contact_email", "created_at", "currency", "local_currency_amount",
        "local_currency_amount_without_tax", "local_currency_subtotal_price",
        "local_currency_total_discounts_set", "currency_symbol", "current_total_discounts",
        "current_total_discounts_set", "current_total_price", "current_total_price_set",
        "current_subtotal_price", "current_subtotal_price_set", "current_total_tax",
        "current_total_tax_set", "customer_locale", "discount_codes", "email",
        "financial_status", "fulfillment_status", "landing_site", "location_id", "name",
        "note", "custom_notes", "note_attributes", "number", "order_number",
        "order_status_url", "payment.gateway","payment.payment_type","payment_details" "payment_brand", "phone",
        "presentment_currency", "processed_at", "processing_method", "referring_site",
        "source_name", "subtotal_price", "subtotal_price_set", "tags", "tax_lines",
        "taxes_included", "test", "token", "total_discounts", "total_discounts_set",
        "total_line_items_price", "total_line_items_price_set", "total_price",
        "total_price_set", "total_tax", "local_currency_total_tax", "total_tax_set",
        "total_price_without_tax", "total_tip_received", "total_weight", "updated_at",
        "customer.id","customer.first_name","customer.last_name", "shop_slug", "shipping_address.country","shipping_address.house_no","shipping_address.address", "shipping_address.province_code",
        "shipping_address.zip", "shipping_address.country_code", "shipping_address.city","shipping_address.neighborhood","shipping_address.phone" # <- adicionamos o campo aqui explicitamente
    ]

    # Cria DataFrame com pedidos
    df_orders = pd.json_normalize(all_orders, sep='.')

    # Garante que a coluna shop_slug esteja presente
    if 'shop_slug' not in df_orders.columns:
        df_orders['shop_slug'] = [o.get('shop_slug') for o in all_orders]

    # Filtra apenas os campos desejados (inclusive shop_slug)
    df_orders_filtered = df_orders[[col for col in selected_fields if col in df_orders.columns]]
    
    # dropa ids nulos
    df_orders_filtered = df_orders_filtered.dropna(subset=['id'])

    # cria uma coluna para pegar a ultima data de att
    saopaulo_tz = pytz.timezone('America/Sao_Paulo')
    df_orders_filtered['ultima_atualizacao'] = datetime.now(saopaulo_tz)
    
        
    # drop duplicated for columns id
    df_orders_filtered = df_orders_filtered.drop_duplicates(subset=['id']) 

    # Extração dos produtos por pedido (line_items)
    line_items_data = []

    for order in all_orders:
        order_id = order.get("id")
        slug = order.get("shop_slug")  # leva o slug para os produtos também

        for item in order.get("line_items", []):
            line_items_data.append({
                "order_id": order_id,
                "item_id": item.get("id"),
                "product_name": item.get("name"),
                "title": item.get("title"),
                "price": item.get("price"),
                "quantity": item.get("quantity"),
                "sku": item.get("sku"),
                "vendor": item.get("vendor"),
                "currency_symbol": item.get("currency_symbol"),
                "total_price": item.get("total_price"),
                "product_main_image": item.get("product_main_image"),
                "shop_slug": slug  # adiciona no produto também
            })

    df_items = pd.DataFrame(line_items_data)

    return {
        "orders_df": df_orders_filtered,
        "items_df": df_items
    }