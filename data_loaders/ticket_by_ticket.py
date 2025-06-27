import pandas as pd
import requests
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def fetch_ticket_details(*args, **kwargs):
    """
    Recebe uma lista de IDs e retorna um DataFrame com os detalhes de cada ticket.
    """
    ticket_ids = args[0]  # lista de IDs vinda do bloco anterior
    token = "0d94a20f-8535-4db0-94c5-ac3c2fe36f80"

    base_url = "https://api.movidesk.com/public/v1/tickets"
    all_tickets = []

    for ticket_id in ticket_ids:
        url = f"{base_url}?token={token}&id={ticket_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list) and len(data) > 0:
                all_tickets.append(data[0])
            elif isinstance(data, dict):
                all_tickets.append(data)
        except Exception as e:
            print(f"Erro ao buscar ticket {ticket_id}: {e}")
        
        time.sleep(0.2)  # respeita a limitação da API

    return pd.DataFrame(all_tickets)


@test
def test_output(output, *args) -> None:
    """
    Verifica se a saída é um DataFrame válido.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pd.DataFrame), 'A saída não é um DataFrame'
    assert 'id' in output.columns, 'Coluna "id" não encontrada no resultado'