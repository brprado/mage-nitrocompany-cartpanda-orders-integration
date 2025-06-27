import pandas as pd
import requests
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    1. Carrega todos os tickets paginando de 500 em 500 (só com campos básicos).
    2. Em seguida, busca detalhes ticket por ticket com base no ID.
    3. Retorna DataFrame com os dados completos, com colunas achatadas.
    """
    # Parte 1: busca paginada de IDs
    paginated_url = (
        "https://api.movidesk.com/public/v1/tickets?"
        "token=0d94a20f-8535-4db0-94c5-ac3c2fe36f80&"
        "$select=id&"
        "$filter=createdDate gt 2025-06-23T00:00:00.00Z& or lastUpdated gt 2025-06-23T00:00:00.00Z&"
        "$orderby=id desc&$top=500&$skip={skip}"
    )

    all_ids = []
    skip = 0
    while True:
        url = paginated_url.format(skip=skip)
        print(f"Fetching IDs from: {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if not data:
                break

            ids_page = [ticket['id'] for ticket in data if 'id' in ticket]
            all_ids.extend(ids_page)
            skip += 500

            time.sleep(0.4)
        except Exception as e:
            print(f"Erro ao buscar IDs (skip={skip}): {e}")
            break

    total_ids = len(all_ids)
    print(f"\n🔄 Total de IDs encontrados: {total_ids}\n")

    # Parte 2: busca detalhe ticket a ticket com barra de progresso
    detail_url_template = "https://api.movidesk.com/public/v1/tickets?token=0d94a20f-8535-4db0-94c5-ac3c2fe36f80&id={ticket_id}"
    all_tickets = []

    for idx, ticket_id in enumerate(all_ids, 1):
        url = detail_url_template.format(ticket_id=ticket_id)
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list) and len(data) > 0:
                all_tickets.append(data[0])
            elif isinstance(data, dict):
                all_tickets.append(data)
        except Exception as e:
            print(f"[{idx}/{total_ids}] ❌ Erro ao buscar ticket {ticket_id}: {e}")
            continue

        print(f"[{idx}/{total_ids}] ✅ Ticket {ticket_id} carregado")
        # time.sleep(0.1)

    return pd.json_normalize(all_tickets)


@test
def test_output(output, *args) -> None:
    """
    Valida se os dados foram carregados corretamente.
    """
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'O DataFrame está vazio'
    assert 'id' in output.columns, 'Coluna "id" ausente'