# app/services/bsale_client.py
import requests
import time
from typing import List, Dict, Any

from app.core.config import settings

class BsaleClient:
    def fetch(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """
        Realiza una petición GET canónica a la API de Bsale.
        endpoint: ruta relativa (ejemplo: 'price_lists/2/details.json')
        params: diccionario de parámetros para la consulta
        Devuelve el JSON decodificado o None en caso de error.
        """
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"[BsaleClient.fetch] Error HTTP al consultar {url} con params {params}: {http_err}")
            print(f"Respuesta: {response.text}")
            return None
        except Exception as err:
            print(f"[BsaleClient.fetch] Error inesperado al consultar {url}: {err}")
            return None
    def __init__(self):
        self.base_url = "https://api.bsale.io/v1"
        self.headers = {
            'access_token': settings.BSALE_API_TOKEN,
            'Content-Type': 'application/json'
        }

    def _get_all_pages(self, endpoint: str, params: Dict = None) -> List[Dict[str, Any]]:
        all_items = []
        url = f"{self.base_url}/{endpoint}"
        offset = 0
        limit = 100

        while True:
            try:
                current_params = params.copy() if params else {}
                current_params.update({'limit': limit, 'offset': offset})
    
                response = requests.get(url, headers=self.headers, params=current_params, timeout=60)
                response.raise_for_status()
                data = response.json()

                items = data.get('items', [])
                if not items:
                    break

                all_items.extend(items)
                offset += len(items)
                time.sleep(0.2)
            except requests.exceptions.HTTPError as http_err:
                print(f"Error HTTP al consultar {url} con params {current_params}: {http_err}")
                print(f"Respuesta: {response.text}")
                return [] # Devuelve lista vacía en caso de error
            except Exception as err:
                print(f"Ocurrió un error inesperado al consultar {url}: {err}")
                return [] # Devuelve lista vacía en caso de error
        
        return all_items

    def get_documents(self, start_date: str = None) -> List[Dict[str, Any]]:
        params = {'expand': 'details'}
        if start_date:
            # Lógica de fecha (si la necesitas)
            pass
        return self._get_all_pages("documents.json", params=params)

    def get_clients(self) -> List[Dict[str, Any]]:
        return self._get_all_pages("clients.json")

    def get_products(self) -> List[Dict[str, Any]]:
        return self._get_all_pages("products.json")

bsale_client = BsaleClient()