import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

# Concurrent API Requests (Paralelismo)
def fetch_customers_for_slug(slug, headers):
    API_URL = f'https://accounts.cartpanda.com/api/v3/{slug}/customers'
    page = 1
    all_customers = []
    
    while True:
        params = {'page': page, 'limit': 200}
        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        customers = data.get('customers', [])
        meta = data.get('meta', {})
        
        print(f"→ {slug} | Página {page}: {len(customers)} clientes")
        
        # Adiciona o shop_slug a cada cliente
        for customer in customers:
            customer['shop_slug'] = slug
            
        all_customers.extend(customers)
        
        # Verifica se chegou na última página
        if meta.get('current_page', page) >= meta.get('last_page', page):
            break
            
        page += 1
        sleep(1)  # Pausa para evitar rate limiting
        
    return all_customers

@data_loader
def cartpanda_customers_extraction(*args, **kwargs):
    slugs = ['vita-waves', 'nutra-force-wl', 'nutra-force-di', 'nutra-force']
    
    API_KEY = get_secret_value('CARTPANDA_API_KEY')
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Accept': 'application/json',
    }
    
    all_customers = []
    
    with ThreadPoolExecutor(max_workers=len(slugs)) as executor:
        # Submete todas as tarefas
        future_to_slug = {executor.submit(fetch_customers_for_slug, slug, headers): slug for slug in slugs}
        
        # Processa os resultados conforme ficam prontos
        for future in as_completed(future_to_slug):
            slug = future_to_slug[future]
            try:
                customers = future.result()
                print(f"✅ Coleta finalizada para: {slug} ({len(customers)} clientes)")
                all_customers.extend(customers)
            except Exception as e:
                print(f"❌ Erro ao coletar clientes da loja {slug}: {e}")
    
    print(f"\n👥 Total geral de clientes coletados: {len(all_customers)}")
    return all_customers