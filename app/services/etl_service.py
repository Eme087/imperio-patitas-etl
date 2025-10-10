# app/services/etl_service.py
from sqlalchemy.orm import Session
from datetime import datetime
from sqlalchemy.dialects.mysql import insert
from app.db import models
from app.services.bsale_client import bsale_client


def sync_clients(db: Session):
    print("Iniciando sincronización de Clientes...")
    clients_list = bsale_client.get_clients()
    if not clients_list:
        print("No se encontraron clientes.")
        return

    clients_to_load = [
        {
            "id_bsale": c.get("id"),
            "nombre": c.get("firstName") or "Sin Nombre",
            "apellido": c.get("lastName"),
            "rut": c.get("code"),
            "email": c.get("email"),
            "telefono": c.get("phone"),
            "direccion": c.get("address"),
            # Convierte el timestamp de Unix a datetime, manejando valores nulos.
            "fecha_creacion": (
                datetime.fromtimestamp(c.get("creationDate")) if c.get("creationDate") is not None else None
            ),
        }
        for c in clients_list
    ]
    if not clients_to_load:
        print("No hay clientes para cargar después de la transformación.")
        return

    stmt = insert(models.Cliente).values(clients_to_load)
    on_duplicate_key_stmt = stmt.on_duplicate_key_update(
        nombre=stmt.inserted.nombre, apellido=stmt.inserted.apellido,
        rut=stmt.inserted.rut, email=stmt.inserted.email,
        telefono=stmt.inserted.telefono, direccion=stmt.inserted.direccion, # Corregido para coincidir con el modelo
    )
    db.execute(on_duplicate_key_stmt)
    print(f"Sincronización de Clientes finalizada. {len(clients_to_load)} registros procesados y guardados.")


def sync_products(db: Session):
    print("\nIniciando sincronización de Productos...")
    # 1. Obtenemos productos expandiendo variantes y, dentro de ellas, precios y costos.
    # Esta es la optimización clave: una sola consulta para toda la información.
    products_list = bsale_client._get_all_pages("products.json", params={'expand': '[variants[pricelists,costs]]'})

    if not products_list:
        print("No se encontraron productos para sincronizar.")
        return

    products_to_load = []
    for p in products_list:
        # 2. Iteramos sobre las variantes de cada producto.
        variants = p.get("variants", {}).get("items", [])
        if not variants:
            # print(f"INFO: Producto ID {p.get('id')} ('{p.get('name')}') no tiene variantes. Se omitirá.")
            continue

        # 2. Buscamos la PRIMERA variante activa (state: 0 en Bsale)
        for variant in variants:
            # Procesamos solo variantes activas (state: 0 en Bsale).
            if variant.get("state") != 0:
                continue
                continue  # Si no está activa, pasamos a la siguiente variante

            # 3. Extraemos los datos comerciales directamente desde la variante expandida.
            pricelists_data = variant.get("pricelists", {}) or {}
            price_list_items = pricelists_data.get("items", [])
            # Tomamos el precio de la primera lista de precios, con un valor por defecto.
            net_price = price_list_items[0].get("netValue") if price_list_items else 0.0

            products_to_load.append({
                "id_bsale": variant.get("id"), # Usamos el ID de la variante como PK
                "nombre": p.get("name"), # Nombre del producto padre
                "id_bsale": variant.get("id"),  # Usamos el ID de la variante como PK
                "nombre": p.get("name"),  # Nombre del producto padre
                "descripcion": p.get("description"),
                "codigo_sku": variant.get("code"), # SKU desde la variante
                "codigo_sku": variant.get("code"),  # SKU desde la variante
                "codigo_barras": variant.get("barCode"),
                "controla_stock": 1 if variant.get("track") else 0,
                "precio_neto": net_price or 0.0,
                "costo_neto": (variant.get("costs") or {}).get("netCost") or 0.0,
                "estado": 1, # Si la variante tiene state 0, la guardamos como activa (1)
                "estado": 1,  # Si la variante tiene state 0, la guardamos como activa (1)
            })
            break  # ¡Clave! Salimos del bucle de variantes una vez que encontramos y procesamos la primera activa.

    if products_to_load:
        print(f"Transformación finalizada. Se cargarán {len(products_to_load)} productos/variantes.")
        stmt = insert(models.Producto).values(products_to_load)
        on_duplicate_key_stmt = stmt.on_duplicate_key_update(
            nombre=stmt.inserted.nombre, descripcion=stmt.inserted.descripcion,
            codigo_sku=stmt.inserted.codigo_sku, codigo_barras=stmt.inserted.codigo_barras,
            controla_stock=stmt.inserted.controla_stock, precio_neto=stmt.inserted.precio_neto,
            costo_neto=stmt.inserted.costo_neto, estado=stmt.inserted.estado,
        )
        db.execute(on_duplicate_key_stmt)
        print(f"Sincronización de Productos finalizada. {len(products_to_load)} registros procesados y guardados.")
    else:
        print("No hay productos para cargar después de la transformación.")


def sync_documents(db: Session, start_date: str = None):
    print(f"\nIniciando sincronización de Documentos de Venta (desde {start_date or 'el inicio'})...")
    try:
        documents_list = bsale_client.get_documents(start_date=start_date)
    except Exception as e:
        import traceback
        print("!!! ERROR al obtener documentos desde Bsale !!!")
        traceback.print_exc()
        return

    if not documents_list:
        print("No se encontraron documentos de venta.")
        return

    print(f"Se encontraron {len(documents_list)} documentos. Procesando en lotes...")
    CHUNK_SIZE = 500

    # 1. Obtenemos todos los IDs de clientes existentes en nuestra BD para validar las FK.
    existing_client_ids = {c.id_bsale for c in db.query(models.Cliente.id_bsale).all()}
    # Obtenemos también los IDs de productos para validar los detalles.
    existing_product_ids = {p.id_bsale for p in db.query(models.Producto.id_bsale).all()}
    print(f"Se encontraron {len(existing_client_ids)} clientes existentes en la base de datos para validación.")

    for i in range(0, len(documents_list), CHUNK_SIZE):
        chunk = documents_list[i:i + CHUNK_SIZE]
        print(f"--- Procesando lote {i // CHUNK_SIZE + 1} de documentos... ---")
        try:
            documents_to_load = [
                {
                    "id_bsale": d.get("id"),
                    "id_cliente": (
                        (d.get("client") or {}).get("id")
                        if (d.get("client") or {}).get("id") in existing_client_ids
                        else None
                    ),
                    "id_tipo_documento": (d.get("documentType") or {}).get("id"),
                    "folio": d.get("number"),
                    "fecha_emision": (
                        datetime.fromtimestamp(d.get("emissionDate")) if d.get("emissionDate") is not None else None
                    ),
                    "monto_neto": d.get("netAmount"),
                    "monto_iva": d.get("taxAmount"),
                    "monto_total": d.get("totalAmount"),
                }
                # 3. Solo filtramos por fecha de emisión, que es un campo crítico y no nullable.
                # Ya no descartamos documentos por no tener cliente.
                for d in chunk if d.get("emissionDate") is not None
            ]

            if documents_to_load:
                stmt_docs = insert(models.DocumentoVenta).values(documents_to_load)
                on_duplicate_key_stmt_docs = stmt_docs.on_duplicate_key_update(
                    id_cliente=stmt_docs.inserted.id_cliente,
                    monto_neto=stmt_docs.inserted.monto_neto,
                    monto_iva=stmt_docs.inserted.monto_iva, monto_total=stmt_docs.inserted.monto_total,
                )
                db.execute(on_duplicate_key_stmt_docs)

            # --- Construcción segura de detalles ---
            all_details_to_load = []
            for doc in chunk:
                doc_id = doc.get("id")
                details = doc.get("details", {}).get("items", [])
                for detail in details:
                    all_details_to_load.append({
                        "id_documento": doc_id,
                        "id_producto": (
                            (detail.get("variant") or {}).get("id")
                            if (detail.get("variant") or {}).get("id") in existing_product_ids
                            else None
                        ),
                        # --- INICIO DE CORRECCIÓN DEFINITIVA ---
                        # Se eliminan las claves duplicadas y se aplica la lógica robusta para evitar valores nulos.
                        # Se implementa una lógica robusta para evitar valores nulos en campos numéricos.
                        # Usamos .get(key, 0.0) y 'or 0.0' para manejar tanto campos ausentes como campos con valor 'None'.
                        # Esto soluciona el IntegrityError: (1048, "Column '...' cannot be null").
                        "cantidad": detail.get("quantity", 0.0) or 0.0,
                        "precio_neto_unitario": detail.get("netUnitValue", 0.0) or 0.0,
                        "descuento_porcentual": detail.get("discount", 0.0) or 0.0,
                        # Esta es la corrección clave para el IntegrityError. Se usa 'netTotal' de la API.
                        "monto_total_linea": detail.get("netTotal", 0.0) or 0.0,
                    })

            if all_details_to_load:
                stmt_details = insert(models.DetalleDocumento).values(all_details_to_load)
                on_duplicate_key_stmt_details = stmt_details.on_duplicate_key_update(
                    cantidad=stmt_details.inserted.cantidad,
                    precio_neto_unitario=stmt_details.inserted.precio_neto_unitario,
                    descuento_porcentual=stmt_details.inserted.descuento_porcentual,
                    monto_total_linea=stmt_details.inserted.monto_total_linea,
                )
                db.execute(on_duplicate_key_stmt_details)

            db.commit() # <-- COMMIT POR CADA LOTE DE DOCUMENTOS
            print(f"--- Lote {i // CHUNK_SIZE + 1} procesado y guardado. ---")
        except Exception as e:
            import traceback
            print(f"!!! ERROR interno al procesar o guardar el lote {i // CHUNK_SIZE + 1} !!!")
            traceback.print_exc()
            db.rollback()
            continue

    print(f"\nSincronización de Documentos finalizada. {len(documents_list)} registros procesados.")