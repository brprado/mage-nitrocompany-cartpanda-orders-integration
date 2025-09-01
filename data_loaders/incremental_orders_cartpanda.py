import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from datetime import datetime, timedelta
import pytz
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

def get_updated_at_min():
    """
    Calcula o timestamp para buscar todos os registros atualizados desde a meia-noite do dia atual (Brasil)
    """
    # Fuso horário do Brasil
    brazil_tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(brazil_tz)
    
    # Meia-noite do dia atual no Brasil
    midnight_today_brazil = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Converte para UTC para a API
    midnight_utc = midnight_today_brazil.astimezone(pytz.UTC)
    
    # Formato ISO 8601 que a API CartPanda espera
    return midnight_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def fetch_orders_for_slug(slug, headers, updated_at_min=None):
    """
    Busca pedidos de uma loja específica, opcionalmente filtrados por data de atualização
    """
    API_URL = f'https://accounts.cartpanda.com/api/v3/{slug}/orders'
    page = 1
    all_orders = []

    while True:
        params = {'page': page, 'limit': 200}
        
        # Adiciona filtro de data se fornecido
        if updated_at_min:
            params['updated_at_min'] = updated_at_min

        try:
            response = requests.get(API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição para {slug} página {page}: {e}")
            break

        data = response.json()
        orders = data.get('orders', [])
        meta = data.get('meta', {})

        brazil_date = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y')
        filter_info = f" (desde meia-noite BR: {brazil_date})" if updated_at_min else ""
        print(f"→ {slug} | Página {page}: {len(orders)} pedidos{filter_info}")

        # Adiciona o shop_slug a cada pedido
        for order in orders:
            order['shop_slug'] = slug

        all_orders.extend(orders)

        # Verifica se chegou na última página
        current_page = meta.get('current_page', page)
        last_page = meta.get('last_page', page)
        
        if current_page >= last_page:
            break
        
        page += 1
        sleep(0.5)  # Reduzido para 500ms já que fazemos menos requisições

    return all_orders

@data_loader
def cartpanda_orders_extraction(*args, **kwargs):
    """
    Extrai pedidos do CartPanda com filtro incremental baseado em updated_at_min
    Retorna sempre uma estrutura válida, mesmo quando não há dados
    """
    slugs = ['vita-waves', 'nutra-force-wl', 'nutra-force-di', 'nutra-force', 'vita-labs']
    API_KEY = get_secret_value('CARTPANDA_API_KEY')
    
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    # Calcula o timestamp para extração desde meia-noite (Brasil)
    updated_at_min = get_updated_at_min()
    
    # Mostra info em horário do Brasil para debug
    brazil_tz = pytz.timezone('America/Sao_Paulo')
    brazil_time = datetime.now(brazil_tz)
    print(f"🔄 Executando extração desde meia-noite do Brasil ({brazil_time.strftime('%d/%m/%Y')})")
    print(f"   Timestamp UTC enviado para API: {updated_at_min}")
    
    # Variável para controlar se é primeira execução (para testes)
    # Se quiser fazer carga completa, descomente a linha abaixo:
    # updated_at_min = None
    
    all_orders = []

    # Execução paralela para todas as lojas
    with ThreadPoolExecutor(max_workers=len(slugs)) as executor:
        future_to_slug = {
            executor.submit(fetch_orders_for_slug, slug, headers, updated_at_min): slug 
            for slug in slugs
        }

        for future in as_completed(future_to_slug):
            slug = future_to_slug[future]
            try:
                orders = future.result()
                if orders:
                    print(f"✅ {slug}: {len(orders)} pedidos coletados")
                    all_orders.extend(orders)
                else:
                    print(f"ℹ️  {slug}: Nenhum pedido novo/atualizado")
                    
            except Exception as e:
                print(f"❌ Erro ao coletar pedidos de {slug}: {e}")

    # Log final e preparação do retorno
    if all_orders:
        print(f"\n📦 Total de pedidos coletados: {len(all_orders)}")
        
        # Mostra range de datas dos pedidos coletados
        if len(all_orders) > 0:
            dates = []
            for order in all_orders:
                if 'updated_at' in order and order['updated_at']:
                    dates.append(order['updated_at'])
            
            if dates:
                dates.sort()
                print(f"📅 Range de updated_at: {dates[0]} até {dates[-1]}")
        
        # Retorna os dados normalmente quando há registros
        return all_orders
    else:
        brazil_date = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%d/%m/%Y')
        print(f"\nℹ️  Nenhum pedido novo/atualizado encontrado desde meia-noite do Brasil ({brazil_date})")
        print("✨ Pipeline será encerrado graciosamente - nenhum dado para processar")
        
        # SOLUÇÃO 1: Retorna lista vazia (mais simples)
        # return []
        
        # SOLUÇÃO 2: Retorna estrutura que sinaliza para próximas etapas que não há dados
        return {
            'execution_metadata': {
                'has_data': False,
                'extraction_timestamp': brazil_time.isoformat(),
                'extraction_date': brazil_date,
                'message': 'Nenhum registro encontrado para o período especificado'
            },
            'orders': [],  # Lista vazia de pedidos
            'total_orders': 0
        }