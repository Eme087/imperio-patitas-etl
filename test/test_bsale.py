# tests/test_bsale.py
import os
import sys
import logging

# Configuración básica de logging para ver información útil en la consola.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuración del Path ---
# Esto es crucial para que Python pueda encontrar los módulos de tu aplicación
# (como 'app.services') cuando ejecutas el test desde la carpeta raíz.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.bsale_client import bsale_client

def test_print_single_product_info():
    """
    Busca un producto específico en Bsale, extrae el SKU, precio, costo y stock
    de su primera variante activa y lo imprime en la consola.
    """
    # --- PARÁMETROS DE LA PRUEBA (Puedes cambiar estos valores) ---
    PRODUCT_ID_TO_TEST = 150
    PRICE_LIST_ID = 1

    logging.info(f"Iniciando extracción para el producto con ID: {PRODUCT_ID_TO_TEST}")

    # 1. OBTENER PRODUCTO, VARIANTES, COSTOS Y STOCK EN UNA SOLA LLAMADA
    # CORRECCIÓN: Usamos el método `_get_all_pages` que sí existe en el cliente.
    # Funciona tanto para obtener listas paginadas como para objetos únicos.
    product = bsale_client._get_all_pages(f"products/{PRODUCT_ID_TO_TEST}.json", params={'expand': '[variants,variants.costs,variants.stock]'})
    if not product:
        logging.error(f"No se pudo encontrar el producto con ID {PRODUCT_ID_TO_TEST}. Abortando.")
        return

    # 2. ENCONTRAR LA PRIMERA VARIANTE ACTIVA
    # En Bsale, un estado '0' significa que está activo.
    active_variant = None
    if product.get("variants") and product["variants"].get("items"):
        for v in product["variants"]["items"]:
            if v.get("state") == 0:
                active_variant = v
                break

    if not active_variant:
        logging.warning(f"El producto '{product.get('name')}' no tiene variantes activas. No se puede continuar.")
        return

    variant_id = active_variant.get("id")
    logging.info(f"Variante activa encontrada con ID: {variant_id} y SKU: {active_variant.get('code')}")

    # 3. EXTRAER PRECIO, COSTO, STOCK Y SKU
    # El SKU, Costo y Stock ya vienen en el objeto 'active_variant' gracias al 'expand'.
    sku = active_variant.get('code', 'N/A')

    # El costo puede ser None o un string. Lo convertimos a float de forma segura.
    cost_str = active_variant.get('costs', {}).get('averageCost')
    cost = float(cost_str) if cost_str else 0.0

    # El stock se obtiene directamente de la variante expandida.
    stock = active_variant.get('stock', {}).get('quantityAvailable', 0.0)

    # Para el Precio, consultamos la lista de precios.
    price_details = bsale_client._get_all_pages(f"price_lists/{PRICE_LIST_ID}/details.json", params={'variantid': variant_id})
    price = price_details['items'][0]['variantValueWithTaxes'] if price_details.get('count', 0) > 0 else 0.0

    # 4. IMPRIMIR RESULTADOS EN PANTALLA
    print("\n" + "="*40)
    print("      INFORMACIÓN DEL PRODUCTO EXTRAÍDA")
    print("="*40)
    print(f"  Producto:         '{product.get('name')}' (ID: {product.get('id')})")
    print(f"  SKU de Variante:  {sku}")
    print(f"  Precio de Venta:  ${price:,.2f}")
    print(f"  Costo Promedio:   ${cost:,.2f}")
    print(f"  Stock Disponible: {stock}")
    print("="*40 + "\n")