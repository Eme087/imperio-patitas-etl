# app/services/etl_service.py
from app.services.bsale_client import bsale_client
from typing import Any
from typing import Any


def _is_bigquery_writer(db: Any) -> bool:
    # Nuestro wrapper expone `insert_rows(table_name, rows)`
    return hasattr(db, "insert_rows") and callable(getattr(db, "insert_rows"))


def sync_clients(db: Session):
    # Iniciando sincronización de Clientes...
    clients_list = bsale_client.get_clients()
    if not clients_list:
    # No se encontraron clientes.
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
    # No hay clientes para cargar después de la transformación.
        return
    # Si el objeto 'db' es un BigQuery writer, hacemos streaming insert a la tabla `cliente`.
    if _is_bigquery_writer(db):
        rows = []
        for c in clients_to_load:
            rows.append({
                "apellido": c.get("apellido"),
                "direccion": c.get("direccion"),
                "email": c.get("email"),
                # BigQuery acepta RFC3339 timestamps; usamos isoformat si existe
                "fecha_creacion": c.get("fecha_creacion").isoformat() if c.get("fecha_creacion") is not None else None,
                "id_bsale": c.get("id_bsale"),
                "nombre": c.get("nombre"),
                "rut": c.get("rut"),
                "telefono": c.get("telefono"),
            })
        if rows:
            db.insert_rows("cliente", rows)
    # Sincronización de Clientes finalizada (BigQuery). {len(rows)} registros procesados y guardados.
        return

    # Modo SQLAlchemy / MySQL (legacy)
    stmt = insert(models.Cliente).values(clients_to_load)
    on_duplicate_key_stmt = stmt.on_duplicate_key_update(
        nombre=stmt.inserted.nombre, apellido=stmt.inserted.apellido,
        rut=stmt.inserted.rut, email=stmt.inserted.email,
        telefono=stmt.inserted.telefono, direccion=stmt.inserted.direccion, # Corregido para coincidir con el modelo
    )
    db.execute(on_duplicate_key_stmt)
    # Sincronización de Clientes finalizada. {len(clients_to_load)} registros procesados y guardados.


def sync_products(db: Session):
    # Iniciando sincronización de Productos...
    # 1. Obtenemos productos expandiendo variantes y, dentro de ellas, precios y costos.
    # El parámetro 'expand' es sensible a mayúsculas. Usamos 'priceLists' (con 'L' mayúscula)
    # para asegurar que la API devuelva la información de precios.
    products_list = bsale_client._get_all_pages("products.json", params={'expand': '[variants.costs]'})

    if not products_list:
    # No se encontraron productos para sincronizar.
        return

    products_to_load = []
    for p in products_list:
        variants = p.get("variants", {}).get("items", [])
        if not variants:
            continue

        for variant in variants:
            # Solo procesamos variantes activas
            if variant.get("state") != 0:
                continue

            # El costo viene directamente en el nodo "costs" de la variante.
            net_cost = (variant.get("costs", {}).get("netCost") if variant.get("costs") else 0.0) or 0.0

            # --- NUEVO: Consultar precio en lista 2 ---
            price_detail = bsale_client.fetch("price_lists/2/details.json", params={"variantid": variant.get("id")})
            price_items = price_detail.get("items", []) if price_detail else []
            if not price_items:
                # ERROR: No existe precio en lista 2 para variante {variant.get('id')} (SKU: {variant.get('code')}) del producto '{p.get('name')}' (ID: {p.get('id')})
                # Deteniendo proceso por integridad de datos.
                raise Exception(f"No existe precio en lista 2 para variante {variant.get('id')}")

            # Tomamos el primer item (debería ser único por variante)
            net_price = price_items[0].get("variantValue")
            if net_price is None:
                # ERROR: El detalle de precio no tiene 'variantValue' para variante {variant.get('id')} (SKU: {variant.get('code')})
                # Deteniendo proceso por integridad de datos.
                raise Exception(f"No existe 'variantValue' en detalle de lista 2 para variante {variant.get('id')}")

            products_to_load.append({
                "id_bsale": variant.get("id"),
                "nombre": p.get("name"),
                "descripcion": p.get("description"),
                "codigo_sku": variant.get("code"),
                "codigo_barras": variant.get("barCode"),
                "controla_stock": 1 if variant.get("track") else 0,
                "precio_neto": net_price,
                "costo_neto": net_cost,
                "estado": 1,
            })
            break  # Procesamos solo la primera variante activa

    if products_to_load:
    # Transformación finalizada. Se cargarán {len(products_to_load)} productos/variantes.
        # BigQuery path
        if _is_bigquery_writer(db):
            rows = []
            for p in products_to_load:
                rows.append({
                    "codigo_barras": p.get("codigo_barras"),
                    "codigo_sku": p.get("codigo_sku"),
                    "controla_stock": int(p.get("controla_stock") or 0),
                    "costo_neto": float(p.get("costo_neto") or 0.0),
                    "descripcion": p.get("descripcion"),
                    "estado": int(p.get("estado") or 0),
                    "id_bsale": p.get("id_bsale"),
                    # Campos opcionales que no tenemos en la API se dejan como NULL
                    "id_marca": None,
                    "id_tipo_producto": None,
                    "nombre": p.get("nombre"),
                    "precio_neto": float(p.get("precio_neto") or 0.0),
                    "stock_disponible": None,
                })
            if rows:
                db.insert_rows("producto", rows)
            # Sincronización de Productos finalizada (BigQuery). {len(rows)} registros procesados y guardados.
            return

        # Modo SQLAlchemy / MySQL (legacy)
        stmt = insert(models.Producto).values(products_to_load)
        on_duplicate_key_stmt = stmt.on_duplicate_key_update(
            nombre=stmt.inserted.nombre, descripcion=stmt.inserted.descripcion,
            codigo_sku=stmt.inserted.codigo_sku, codigo_barras=stmt.inserted.codigo_barras,
            controla_stock=stmt.inserted.controla_stock, precio_neto=stmt.inserted.precio_neto,
            costo_neto=stmt.inserted.costo_neto, estado=stmt.inserted.estado,
        )
        db.execute(on_duplicate_key_stmt)
    # Sincronización de Productos finalizada. {len(products_to_load)} registros procesados y guardados.
    else:
    # No hay productos para cargar después de la transformación.


def sync_documents(db: Session, start_date: str = None):
    # Iniciando sincronización de Documentos de Venta (desde {start_date or 'el inicio'})...
    try:
        documents_list = bsale_client.get_documents(start_date=start_date)
    except Exception as e:
    # ERROR al obtener documentos desde Bsale
        return

    if not documents_list:
    # No se encontraron documentos de venta.
        return

    # Se encontraron {len(documents_list)} documentos. Procesando en lotes...
    CHUNK_SIZE = 500

    # Si usamos BigQuery, no podemos consultar tablas con el Session SQLAlchemy.
    # En ese caso asumimos que la validación de FK no es estricta y permitimos NULLs cuando no existan.
    if _is_bigquery_writer(db):
        existing_client_ids = None
        existing_product_ids = None
        print("BigQuery mode: se omitirá la validación previa de FK contra la BD local.")
    else:
        # 1. Obtenemos todos los IDs de clientes existentes en nuestra BD para validar las FK.
        existing_client_ids = {c.id_bsale for c in db.query(models.Cliente.id_bsale).all()}
        # Obtenemos también los IDs de productos para validar los detalles.
        existing_product_ids = {p.id_bsale for p in db.query(models.Producto.id_bsale).all()}
        print(f"Se encontraron {len(existing_client_ids)} clientes existentes en la base de datos para validación.")

    for i in range(0, len(documents_list), CHUNK_SIZE):
        chunk = documents_list[i:i + CHUNK_SIZE]
        print(f"--- Procesando lote {i // CHUNK_SIZE + 1} de documentos... ---")
        try:
            # Construimos documentos preparados para la carga
            documents_to_load = []
            for d in chunk:
                if d.get("emissionDate") is None:
                    continue
                documents_to_load.append({
                    "id_bsale": d.get("id"),
                    "id_cliente": (
                        (d.get("client") or {}).get("id")
                        if existing_client_ids is None or (d.get("client") or {}).get("id") in existing_client_ids
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
                })

            # Si usamos BigQuery, insertamos en lote usando el writer
            if _is_bigquery_writer(db):
                # Convertir a filas BQ (timestamps a ISO)
                doc_rows = []
                for d in documents_to_load:
                    doc_rows.append({
                        "fecha_emision": d.get("fecha_emision").isoformat() if d.get("fecha_emision") is not None else None,
                        "folio": d.get("folio"),
                        "id_bsale": d.get("id_bsale"),
                        "id_cliente": d.get("id_cliente"),
                        "id_tipo_documento": d.get("id_tipo_documento"),
                        "monto_iva": float(d.get("monto_iva") or 0.0),
                        "monto_neto": float(d.get("monto_neto") or 0.0),
                        "monto_total": float(d.get("monto_total") or 0.0),
                    })

                if doc_rows:
                    db.insert_rows("documento_venta", doc_rows)

            else:
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
                    prod_id = (detail.get("variant") or {}).get("id")
                    if existing_product_ids is not None and prod_id not in existing_product_ids:
                        prod_id = None
                    all_details_to_load.append({
                        "id_documento": doc_id,
                        "id_producto": prod_id,
                        # Valores numéricos defensivos
                        "cantidad": float(detail.get("quantity", 0.0) or 0.0),
                        "precio_neto_unitario": float(detail.get("netUnitValue", 0.0) or 0.0),
                        "descuento_porcentual": float(detail.get("discount", 0.0) or 0.0),
                        "monto_total_linea": float(detail.get("netTotal", 0.0) or 0.0),
                    })

            if _is_bigquery_writer(db):
                if all_details_to_load:
                    db.insert_rows("detalle_documento", all_details_to_load)
            else:
                if all_details_to_load:
                    stmt_details = insert(models.DetalleDocumento).values(all_details_to_load)
                    on_duplicate_key_stmt_details = stmt_details.on_duplicate_key_update(
                        cantidad=stmt_details.inserted.cantidad,
                        precio_neto_unitario=stmt_details.inserted.precio_neto_unitario,
                        descuento_porcentual=stmt_details.inserted.descuento_porcentual,
                        monto_total_linea=stmt_details.inserted.monto_total_linea,
                    )
                    db.execute(on_duplicate_key_stmt_details)

            # Solo hacemos commit si estamos en SQLAlchemy
            if not _is_bigquery_writer(db):
                db.commit() # <-- COMMIT POR CADA LOTE DE DOCUMENTOS
            print(f"--- Lote {i // CHUNK_SIZE + 1} procesado y guardado. ---")
        except Exception as e:
            import traceback
            print(f"!!! ERROR interno al procesar o guardar el lote {i // CHUNK_SIZE + 1} !!!")
            traceback.print_exc()
            db.rollback()
            continue

    print(f"\nSincronización de Documentos finalizada. {len(documents_list)} registros procesados.")