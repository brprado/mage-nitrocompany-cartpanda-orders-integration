import pandas as pd
from datetime import datetime
import pytz

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform_cartpanda_data(data, *args, **kwargs):
    """
    Transforma dados do CartPanda em DataFrames estruturados
    Trata casos onde não há dados para processar de forma elegante
    """
    print(f"🔍 DEBUG: len(data) = {len(data)}, type(data) = {type(data)}")
    
    # DEBUG: Vamos ver o que está vindo na primeira posição
    if len(data) > 0:
        print(f"🔍 DEBUG: type(data[0]) = {type(data[0])}")
        if hasattr(data[0], 'keys'):
            print(f"🔍 DEBUG: data[0].keys() = {list(data[0].keys())}")
    
    # ETAPA 1: Verificação e tratamento dos dados de entrada
    print("🔄 Iniciando transformação dos dados CartPanda...")
    
    # Caso 1: Dados com estrutura de metadados (Solução 2 do data loader)
    if isinstance(data, dict) and 'execution_metadata' in data:
        print("📋 Recebidos dados com metadados de execução")
        
        # Verifica se há dados para processar
        if not data['execution_metadata']['has_data']:
            print(f"ℹ️  {data['execution_metadata']['message']}")
            print("✨ Transformer será finalizado graciosamente - nenhum dado para transformar")
            
            # Retorna estrutura vazia mas válida para próximos blocos
            empty_orders_df = pd.DataFrame(columns=[
                "id", "status_id", "browser_ip", "buyer_accepts_marketing", "buyer_accepts_phone_marketing",
                "cancel_reason", "cancelled_at", "cart_token", "client_details", "closed_at",
                "contact_email", "created_at", "currency", "local_currency_amount",
                "local_currency_amount_without_tax", "local_currency_subtotal_price",
                "local_currency_total_discounts_set", "currency_symbol", "current_total_discounts",
                "current_total_discounts_set", "current_total_price", "current_total_price_set",
                "current_subtotal_price", "current_subtotal_price_set", "current_total_tax",
                "current_total_tax_set", "customer_locale", "email",
                "financial_status", "fulfillment_status", "landing_site", "location_id", "name",
                "note", "custom_notes", "note_attributes", "number", "order_number",
                "order_status_url", "payment.gateway", "payment.payment_type", "payment_details", 
                "payment_brand", "phone", "presentment_currency", "processed_at", "processing_method", 
                "referring_site", "source_name", "subtotal_price", "subtotal_price_set", "tags", 
                "tax_lines", "taxes_included", "test", "token", "total_discounts", "total_discounts_set",
                "total_line_items_price", "total_line_items_price_set", "total_price",
                "total_price_set", "total_tax", "local_currency_total_tax", "total_tax_set",
                "total_price_without_tax", "total_tip_received", "total_weight", "updated_at",
                "customer.id", "customer.first_name", "customer.last_name", "shop_slug", 
                "shipping_address.country", "shipping_address.house_no", "shipping_address.address",
                "shipping_address.province_code", "shipping_address.zip", "shipping_address.country_code", 
                "shipping_address.city", "shipping_address.neighborhood", "shipping_address.phone", 
                "shipping_lines.local_currency_shipping_price", "discount_codes_local_currency_discount_amount",
                "ultima_atualizacao"
            ])
            
            empty_items_df = pd.DataFrame(columns=[
                "order_id", "item_id", "product_name", "title", "price", "quantity", 
                "sku", "vendor", "currency_symbol", "total_price", "product_main_image", "shop_slug"
            ])
            
            return {
                "orders_df": empty_orders_df,
                "items_df": empty_items_df,
                "execution_metadata": data['execution_metadata']  # Passa metadados adiante
            }
        
        # Se chegou aqui, há dados para processar - extrai os pedidos
        all_orders = data['orders']
        print(f"📦 Processando {len(all_orders)} pedidos dos metadados")
    
    # Caso 2: Lista tradicional de pedidos
    elif isinstance(data, list):
        # Verifica se a lista está vazia
        if not data or len(data) == 0:
            print("ℹ️  Lista de pedidos vazia recebida do extrator")
            print("✨ Transformer será finalizado graciosamente - nenhum dado para transformar")
            
            # Retorna DataFrames vazios mas com estrutura correta
            return {
                "orders_df": pd.DataFrame(),  # DataFrame vazio
                "items_df": pd.DataFrame()   # DataFrame vazio
            }
        
        # CORREÇÃO PRINCIPAL: Verifica se data[0] é o dicionário de metadados
        if len(data) == 1 and isinstance(data[0], dict) and 'execution_metadata' in data[0]:
            print("📋 Detectado dicionário com metadados em data[0]")
            metadata_dict = data[0]
            
            # Verifica se há dados para processar
            if not metadata_dict['execution_metadata']['has_data']:
                print(f"ℹ️  {metadata_dict['execution_metadata']['message']}")
                print("✨ Transformer será finalizado graciosamente - nenhum dado para transformar")
                
                return {
                    "orders_df": pd.DataFrame(),
                    "items_df": pd.DataFrame(),
                    "execution_metadata": metadata_dict['execution_metadata']
                }
            
            # Se há dados, extrai os pedidos
            all_orders = metadata_dict['orders']
            print(f"📦 Processando {len(all_orders)} pedidos extraídos dos metadados")
        else:
            # Lista normal de pedidos
            all_orders = data
            print(f"📦 Processando {len(all_orders)} pedidos da lista")
    
    # Caso 3: Tipo de dado inesperado
    else:
        print(f"⚠️  Tipo de dado inesperado recebido: {type(data)}")
        print("🔄 Tentando processar como lista...")
        
        # Tenta converter para lista ou retorna vazio
        try:
            all_orders = list(data) if data else []
            if not all_orders:
                return {
                    "orders_df": pd.DataFrame(),
                    "items_df": pd.DataFrame()
                }
        except (TypeError, ValueError):
            print("❌ Não foi possível converter dados para lista")
            return {
                "orders_df": pd.DataFrame(),
                "items_df": pd.DataFrame()
            }

    # VALIDAÇÃO FINAL: Garante que all_orders é uma lista de dicionários
    if not isinstance(all_orders, list):
        print(f"❌ all_orders não é uma lista: {type(all_orders)}")
        return {
            "orders_df": pd.DataFrame(),
            "items_df": pd.DataFrame()
        }
    
    # Verifica se os elementos são dicionários
    if all_orders and not isinstance(all_orders[0], dict):
        print(f"❌ Elementos de all_orders não são dicionários: {type(all_orders[0])}")
        return {
            "orders_df": pd.DataFrame(),
            "items_df": pd.DataFrame()
        }
    
    print(f"✅ Validação OK: {len(all_orders)} pedidos válidos para processar")

    # ETAPA 2: Transformação dos dados de pedidos (lógica original mantida)
    print("🔧 Iniciando transformação dos pedidos...")
    
    # Campos principais da tabela de pedidos
    selected_fields = [
        "id","status_id", "browser_ip", "buyer_accepts_marketing", "buyer_accepts_phone_marketing",
        "cancel_reason", "cancelled_at", "cart_token", "client_details", "closed_at",
        "contact_email", "created_at", "currency", "local_currency_amount",
        "local_currency_amount_without_tax", "local_currency_subtotal_price",
        "local_currency_total_discounts_set", "currency_symbol", "current_total_discounts",
        "current_total_discounts_set", "current_total_price", "current_total_price_set",
        "current_subtotal_price", "current_subtotal_price_set", "current_total_tax",
        "current_total_tax_set", "customer_locale", "email",
        "financial_status", "fulfillment_status", "landing_site", "location_id", "name",
        "note", "custom_notes", "note_attributes", "number", "order_number",
        "order_status_url", "payment.gateway","payment.payment_type","payment_details", "payment_brand", "phone",
        "presentment_currency", "processed_at", "processing_method", "referring_site",
        "source_name", "subtotal_price", "subtotal_price_set", "tags", "tax_lines",
        "taxes_included", "test", "token", "total_discounts", "total_discounts_set",
        "total_line_items_price", "total_line_items_price_set", "total_price",
        "total_price_set", "total_tax", "local_currency_total_tax", "total_tax_set",
        "total_price_without_tax", "total_tip_received", "total_weight", "updated_at",
        "customer.id","customer.first_name","customer.last_name", "shop_slug", "shipping_address.country",
        "shipping_address.house_no","shipping_address.address","shipping_address.province_code",
        "shipping_address.zip", "shipping_address.country_code", "shipping_address.city",
        "shipping_address.neighborhood","shipping_address.phone", "shipping_lines.local_currency_shipping_price",
        "discount_codes_local_currency_discount_amount"  # Campo customizado que vamos criar
    ]

    # Cria DataFrame com pedidos
    try:
        df_orders = pd.json_normalize(all_orders, sep='.')
        print(f"✅ DataFrame de pedidos criado com {len(df_orders)} registros")
    except Exception as e:
        print(f"❌ Erro ao normalizar dados JSON: {e}")
        return {
            "orders_df": pd.DataFrame(),
            "items_df": pd.DataFrame()
        }

    # Garante que a coluna shop_slug esteja presente
    if 'shop_slug' not in df_orders.columns:
        df_orders['shop_slug'] = [o.get('shop_slug') for o in all_orders]
        print("🔧 Coluna shop_slug adicionada manualmente")

    # Função para extrair o total de discount_codes.local_currency_discount_amount
    def extract_discount_codes_amount(order):
        """Extrai e soma os valores de desconto dos códigos promocionais"""
        # CORREÇÃO: Validação de entrada
        if not isinstance(order, dict):
            print(f"⚠️  extract_discount_codes_amount recebeu tipo inválido: {type(order)}")
            return 0
            
        total_discount = 0
        discount_codes = order.get("discount_codes", [])
        
        # Se discount_codes for uma lista de objetos
        if isinstance(discount_codes, list) and discount_codes:
            for discount in discount_codes:
                if isinstance(discount, dict):
                    amount = discount.get("local_currency_discount_amount", 0)
                    if amount:
                        # Converte para float, tratando vírgulas como separador decimal
                        try:
                            if isinstance(amount, str):
                                amount = amount.replace(',', '.')
                            total_discount += float(amount)
                        except (ValueError, TypeError):
                            pass
        
        return total_discount

    # Adiciona a coluna customizada - CORREÇÃO: Garante que all_orders seja usado corretamente
    print("🔧 Calculando valores de desconto...")
    try:
        df_orders['discount_codes_local_currency_discount_amount'] = [
            extract_discount_codes_amount(order) for order in all_orders
        ]
        print("✅ Coluna de descontos adicionada com sucesso")
    except Exception as e:
        print(f"❌ Erro ao calcular descontos: {e}")
        # Adiciona coluna com zeros como fallback
        df_orders['discount_codes_local_currency_discount_amount'] = 0

    # Filtra apenas os campos desejados (que existem no DataFrame)
    available_fields = [col for col in selected_fields if col in df_orders.columns]
    df_orders_filtered = df_orders[available_fields]
    print(f"🔧 Filtrados {len(available_fields)} campos disponíveis de {len(selected_fields)} solicitados")
    
    # Remove pedidos com IDs nulos
    initial_count = len(df_orders_filtered)
    df_orders_filtered = df_orders_filtered.dropna(subset=['id'])
    removed_null_ids = initial_count - len(df_orders_filtered)
    if removed_null_ids > 0:
        print(f"🧹 Removidos {removed_null_ids} pedidos com ID nulo")

    # Cria coluna de última atualização
    saopaulo_tz = pytz.timezone('America/Sao_Paulo')
    df_orders_filtered['ultima_atualizacao'] = datetime.now(saopaulo_tz)
    
    # Remove duplicatas por ID
    initial_count = len(df_orders_filtered)
    df_orders_filtered = df_orders_filtered.drop_duplicates(subset=['id'])
    removed_duplicates = initial_count - len(df_orders_filtered)
    if removed_duplicates > 0:
        print(f"🧹 Removidas {removed_duplicates} duplicatas por ID")

    # ETAPA 3: Extração dos produtos por pedido (line_items)
    print("🔧 Processando line_items (produtos dos pedidos)...")
    
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
                "price": item.get("local_currency_item_total_price"),
                "quantity": item.get("quantity"),
                "sku": item.get("sku"),
                "vendor": item.get("vendor"),
                "currency_symbol": item.get("currency_symbol"),
                "total_price": item.get("total_price"),
                "product_main_image": item.get("product_main_image"),
                "shop_slug": slug  # adiciona no produto também
            })

    df_items = pd.DataFrame(line_items_data)
    print(f"✅ DataFrame de itens criado com {len(df_items)} produtos")

    # ETAPA 4: Relatório final e retorno
    print(f"\n📊 RESUMO DA TRANSFORMAÇÃO:")
    print(f"   • Pedidos processados: {len(df_orders_filtered)}")
    print(f"   • Produtos extraídos: {len(df_items)}")
    print(f"   • Campos de pedido: {len(df_orders_filtered.columns)}")
    print(f"   • Última atualização: {datetime.now(saopaulo_tz).strftime('%d/%m/%Y %H:%M:%S')}")

    return {
        "orders_df": df_orders_filtered,
        "items_df": df_items
    }