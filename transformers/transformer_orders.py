import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform_cartpanda_data(data, *args, **kwargs):
    all_orders = data

    # Campos desejados da tabela de pedidos
    selected_fields = [
        "id", "browser_ip", "buyer_accepts_marketing", "buyer_accepts_phone_marketing",
        "cancel_reason", "cancelled_at", "cart_token", "client_details", "closed_at",
        "contact_email", "created_at", "currency", "local_currency_amount",
        "local_currency_amount_without_tax", "local_currency_subtotal_price",
        "local_currency_total_discounts_set", "currency_symbol", "current_total_discounts",
        "current_total_discounts_set", "current_total_price", "current_total_price_set",
        "current_subtotal_price", "current_subtotal_price_set", "current_total_tax",
        "current_total_tax_set", "customer_locale", "discount_codes", "email",
        "financial_status", "fulfillment_status", "landing_site", "location_id", "name",
        "note", "custom_notes", "note_attributes", "number", "order_number",
        "order_status_url", "payment_gateway_names", "payment_brand", "phone",
        "presentment_currency", "processed_at", "processing_method", "referring_site",
        "source_name", "subtotal_price", "subtotal_price_set", "tags", "tax_lines",
        "taxes_included", "test", "token", "total_discounts", "total_discounts_set",
        "total_line_items_price", "total_line_items_price_set", "total_price",
        "total_price_set", "total_tax", "local_currency_total_tax", "total_tax_set",
        "total_price_without_tax", "total_tip_received", "total_weight", "updated_at",
        "user_id"
    ]

    # 🧾 Tabela de pedidos (orders)
    df_orders = pd.json_normalize(all_orders, sep='.')
    df_orders_filtered = df_orders[[col for col in selected_fields if col in df_orders.columns]]

    # 🧾 Tabela de produtos por pedido (line_items)
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

    # Retorna ambos os DataFrames
    return {
        "orders_df": df_orders_filtered,
        "items_df": df_items
    }