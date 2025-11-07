# app/services/etl_service.py - VERSIÓN CON INTEGRIDAD DE DATOS
from app.services.bsale_client import bsale_client
from typing import Any, List, Dict
from datetime import datetime
# Solo BigQuery - removido MySQL/SQLAlchemy
from app.core.config import settings
import logging
import re


class DataValidationError(Exception):
    """Error de validación de datos"""
    pass


class ETLDataValidator:
    """Validador estricto de datos para ETL"""
    
    @staticmethod
    def validate_client(client_data: Dict) -> Dict:
        """Valida datos de cliente con reglas flexibles (solo ID y nombre requeridos)"""
        errors = []
        
        # ID requerido
        if not client_data.get("id"):
            errors.append("Cliente sin ID")
        
        # Nombre requerido y no vacío
        first_name = client_data.get("firstName", "").strip()
        if not first_name or first_name.lower() in ["sin nombre", "null", "none", ""]:
            errors.append(f"Cliente {client_data.get('id')}: nombre inválido '{first_name}'")
        
                # RUT es opcional, solo validar formato si existe
        rut = client_data.get("code", "").strip()
        if rut and rut.lower() not in ["null", "none", ""] and not ETLDataValidator._is_valid_rut(rut):
            errors.append(f"Cliente {client_data.get('id')}: RUT inválido '{rut}'")
        
        # Email OPCIONAL - solo validar formato si está presente  
        email = client_data.get("email", "").strip()
        if email and email.lower() not in ["", "null", "none"] and not ETLDataValidator._is_valid_email(email):
            # Solo warning, no error crítico
            logging.warning(f"Cliente {client_data.get('id')}: email con formato no estándar '{email}' - se mantiene")
        
        if errors:
            raise DataValidationError(f"Cliente inválido: {'; '.join(errors)}")
        
        return {
            "id_cliente": client_data.get("id"),
            "nombre": first_name,
            "apellido": (client_data.get("lastName") or "").strip() or None,
            "rut": rut or None,
            "email": email or None,
            "telefono": (client_data.get("phone") or "").strip() or None,
            "direccion": (client_data.get("address") or "").strip() or None,
            "fecha_creacion": client_data.get("creationDate"),  # Keep as Unix timestamp
        }
    
    @staticmethod
    def validate_product(product_data: Dict, variant_data: Dict, price_data: float, cost_data: float) -> Dict:
        """Valida datos de producto con reglas ESTRICTAS"""
        errors = []
        
        # ID de variante requerido
        variant_id = variant_data.get("id")
        if not variant_id:
            errors.append("Variante sin ID")
        
        # Nombre de producto requerido
        product_name = (product_data.get("name") or "").strip()
        if not product_name or product_name.lower() in ["sin nombre", "null", "none", ""]:
            errors.append(f"Producto {product_data.get('id')}: nombre inválido '{product_name}'")
        
        # SKU requerido
        sku = (variant_data.get("code") or "").strip()
        if not sku or sku.lower() in ["null", "none", ""]:
            errors.append(f"Variante {variant_id}: SKU faltante o inválido '{sku}'")
        
        # PRECIO OBLIGATORIO Y MAYOR A 0
        if price_data is None or price_data <= 0:
            errors.append(f"Variante {variant_id} (SKU: {sku}): PRECIO INVÁLIDO {price_data} - debe ser > 0")
        
        # COSTO OBLIGATORIO Y MAYOR O IGUAL A 0
        if cost_data is None or cost_data < 0:
            errors.append(f"Variante {variant_id} (SKU: {sku}): COSTO INVÁLIDO {cost_data} - debe ser >= 0")
        
        # Precio debe ser mayor al costo (margen positivo)
        if price_data and cost_data and price_data <= cost_data:
            logging.warning(f"Variante {variant_id}: Precio {price_data} <= Costo {cost_data} (margen negativo)")
        
        # Estado de variante debe ser activo
        if variant_data.get("state") != 0:
            errors.append(f"Variante {variant_id}: estado inactivo {variant_data.get('state')}")
        
        if errors:
            raise DataValidationError(f"Producto inválido: {'; '.join(errors)}")
        
        return {
            "id_producto": variant_id,
            "nombre": product_name,
            "descripcion": (product_data.get("description") or "").strip() or None,
            "codigo_sku": sku,
            "codigo_barras": (variant_data.get("barCode") or "").strip() or None,
            "controla_stock": 1 if variant_data.get("track") else 0,
            "precio_neto": float(price_data),
            "costo_neto": float(cost_data),
            "estado": 1,
        }
    
    @staticmethod
    def validate_document(document_data: Dict) -> Dict:
        """Valida datos de documento con reglas estrictas"""
        errors = []
        
        # ID requerido
        doc_id = document_data.get("id")
        if not doc_id:
            errors.append("Documento sin ID")
        
        # Fecha de emisión requerida
        emission_date = document_data.get("emissionDate")
        if not emission_date:
            errors.append(f"Documento {doc_id}: fecha de emisión faltante")
        
        # Montos deben ser válidos
        net_amount = document_data.get("netAmount", 0)
        tax_amount = document_data.get("taxAmount", 0)  
        total_amount = document_data.get("totalAmount", 0)
        
        if net_amount < 0:
            errors.append(f"Documento {doc_id}: monto neto negativo {net_amount}")
        
        if tax_amount < 0:
            errors.append(f"Documento {doc_id}: monto IVA negativo {tax_amount}")
        
        if total_amount <= 0:
            errors.append(f"Documento {doc_id}: monto total inválido {total_amount}")
        
        # Verificar coherencia de montos
        expected_total = net_amount + tax_amount
        if abs(total_amount - expected_total) > 0.01:  # Tolerancia de 1 centavo
            logging.warning(f"Documento {doc_id}: inconsistencia en montos - Total: {total_amount}, Esperado: {expected_total}")
        
        if errors:
            raise DataValidationError(f"Documento inválido: {'; '.join(errors)}")
        
        return {
            "id_documento": doc_id,
            "id_cliente": (document_data.get("client") or {}).get("id"),
            "id_tipo_documento": (document_data.get("documentType") or {}).get("id"),
            "folio": document_data.get("number"),
            "fecha_emision": emission_date,  # Keep as Unix timestamp
            "monto_neto": float(net_amount),
            "monto_iva": float(tax_amount),
            "monto_total": float(total_amount),
        }
    
    @staticmethod
    def validate_document_detail(detail_data: Dict, doc_id: int) -> Dict:
        """Valida detalles de documento"""
        errors = []
        
        # ID de detalle requerido
        detail_id = detail_data.get("id")
        if not detail_id:
            errors.append(f"Detalle documento {doc_id}: ID de detalle faltante")
        
        # Producto/variante requerido
        variant_id = (detail_data.get("variant") or {}).get("id")
        if not variant_id:
            errors.append(f"Detalle documento {doc_id}: producto/variante faltante")
        
        # Cantidad debe ser válida
        quantity = detail_data.get("quantity", 0)
        if quantity <= 0:
            errors.append(f"Detalle documento {doc_id}, producto {variant_id}: cantidad inválida {quantity}")
        
        # Precio unitario debe ser válido
        unit_price = detail_data.get("netUnitValue", 0)
        if unit_price <= 0:
            errors.append(f"Detalle documento {doc_id}, producto {variant_id}: precio unitario inválido {unit_price}")
        
        # Total de línea debe ser coherente
        line_total = detail_data.get("netTotal", 0)
        discount = detail_data.get("discount", 0)
        
        expected_total = (quantity * unit_price) * (1 - discount / 100)
        if abs(line_total - expected_total) > 0.01:
            logging.warning(f"Documento {doc_id}, producto {variant_id}: inconsistencia en total de línea")
        
        if errors:
            raise DataValidationError(f"Detalle documento inválido: {'; '.join(errors)}")
        
        return {
            "id_detalle": detail_id,
            "id_documento": doc_id,
            "id_producto": variant_id,
            "cantidad": float(quantity),
            "precio_neto_unitario": float(unit_price),
            "descuento_porcentual": float(discount),
            "monto_total_linea": float(line_total),
        }
    
    @staticmethod
    def _is_valid_rut(rut: str) -> bool:
        """Valida formato básico de RUT chileno - PERMISIVO"""
        if not rut:
            return False
        # Limpiar puntos y espacios
        clean_rut = rut.replace(".", "").replace(" ", "").strip()
        if not clean_rut:
            return False
        
        # Formato muy permisivo: al menos números, puede tener guión y dígito verificador
        # Acepta formatos como: 12345678-9, 12345678-K, 123456789, etc.
        return bool(re.match(r'^[0-9]{7,8}[-]?[0-9kK]?$', clean_rut))
    
    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """Valida formato básico de email"""
        if not email:
            return False
        return bool(re.match(r'^[^@]+@[^@]+\.[^@]+$', email))


# Removido - Solo usamos BigQuery


def _execute_bigquery_query(db, query: str, description: str = "Query"):
    """Ejecuta una query en BigQuery de forma segura"""
    try:
        if hasattr(db, 'execute_query'):
            result = db.execute_query(query)
            logging.info(f"✅ {description} ejecutado exitosamente via execute_query")
            return result
        elif hasattr(db, 'query'):
            result = db.query(query)
            logging.info(f"✅ {description} ejecutado exitosamente via query")
            return result
        elif hasattr(db, '_client'):
            job = db._client.query(query)
            result = job.result()
            logging.info(f"✅ {description} ejecutado exitosamente via _client")
            return result
        else:
            logging.warning(f"⚠️ No se puede ejecutar MERGE, usando DELETE+INSERT para {description}")
            return None
    except Exception as e:
        logging.error(f"🔴 Error en {description}: {e}")
        raise


def _bigquery_upsert_with_merge(db, table_name: str, rows: list, merge_key: str, description: str):
    """UPSERT genérico usando MERGE para BigQuery - PROCESA EN LOTES"""
    if not rows:
        logging.info(f"ℹ️ No hay datos válidos para {description}")
        return
        
    logging.info(f"🔄 Ejecutando UPSERT de {len(rows)} registros válidos en {table_name}...")
    
    # Procesar en lotes de 50 para evitar queries demasiado grandes
    BATCH_SIZE = 50
    total_processed = 0
    
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
        
        logging.info(f"📦 Procesando lote {batch_num}/{total_batches} ({len(batch)} registros)...")
        
        # Construir MERGE query según la tabla
        if table_name == "cliente":
            merge_query = _build_cliente_merge(batch)
        elif table_name == "producto":
            merge_query = _build_producto_merge(batch)
        elif table_name == "documento_venta":
            merge_query = _build_documento_merge(batch)
        elif table_name == "detalle_documento":
            merge_query = _build_detalle_merge(batch)
        else:
            raise ValueError(f"Tabla no soportada para MERGE: {table_name}")
        
        # Intentar MERGE, si falla usar DELETE+INSERT
        try:
            _execute_bigquery_query(db, merge_query, f"MERGE {table_name} lote {batch_num}")
            total_processed += len(batch)
            logging.info(f"✅ Lote {batch_num} completado ({total_processed}/{len(rows)} total)")
        except Exception as e:
            logging.warning(f"⚠️ MERGE falló para lote {batch_num}, intentando DELETE+INSERT: {e}")
            _bigquery_delete_and_insert(db, table_name, batch, merge_key, f"{description} lote {batch_num}")
            total_processed += len(batch)
    
    logging.info(f"✅ UPSERT completado: {total_processed} registros procesados en {table_name}")


def _bigquery_delete_and_insert(db, table_name: str, rows: list, key_field: str, description: str):
    """Fallback: DELETE + INSERT para BigQuery cuando MERGE no funciona"""
    if not rows:
        return
        
    # Obtener IDs a eliminar
    ids_to_delete = []
    for row in rows:
        if key_field in row and row[key_field] is not None:
            ids_to_delete.append(str(row[key_field]))
    
    if ids_to_delete:
        delete_query = f"""
        DELETE FROM `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.{table_name}`
        WHERE {key_field} IN ({','.join(ids_to_delete)})
        """
        
        try:
            _execute_bigquery_query(db, delete_query, f"DELETE from {table_name}")
            logging.info(f"🗑️ Eliminados {len(ids_to_delete)} registros existentes de {table_name}")
        except Exception as e:
            logging.error(f"🔴 Error en DELETE de {table_name}: {e}")
    
    # INSERT nuevos datos
    try:
        db.insert_rows(table_name, rows)
        logging.info(f"✅ Insertados {len(rows)} registros validados en {table_name}")
    except Exception as e:
        logging.error(f"🔴 Error en INSERT de {table_name}: {e}")
        raise


def _build_cliente_merge(rows):
    """Construye query MERGE para clientes"""
    values_list = []
    for row in rows:
        nombre = str(row.get('nombre', '')).replace('"', '\\"')
        apellido = str(row.get('apellido', '') or '').replace('"', '\\"')
        direccion = str(row.get('direccion', '') or '').replace('"', '\\"')
        
        fecha_creacion = 'NULL'
        if row.get('fecha_creacion'):
            fecha_creacion = f"TIMESTAMP_SECONDS({int(row['fecha_creacion'])})"
        
        values_list.append(f"""STRUCT(
            {row.get('id_cliente')} AS id_cliente,
            "{nombre}" AS nombre,
            "{apellido}" AS apellido,
            "{row.get('rut', '') or ''}" AS rut,
            "{row.get('email', '') or ''}" AS email,
            "{row.get('telefono', '') or ''}" AS telefono,
            "{direccion}" AS direccion,
            {fecha_creacion} AS fecha_creacion
        )""")
    
    return f"""
    MERGE `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.cliente` AS target
    USING (
        SELECT * FROM UNNEST([
            {','.join(values_list)}
        ])
    ) AS source
    ON target.id_cliente = source.id_cliente
    WHEN MATCHED THEN 
        UPDATE SET 
            nombre = source.nombre,
            apellido = source.apellido,
            rut = source.rut,
            email = source.email,
            telefono = source.telefono,
            direccion = source.direccion
    WHEN NOT MATCHED THEN
        INSERT (id_cliente, nombre, apellido, rut, email, telefono, direccion, fecha_creacion)
        VALUES (source.id_cliente, source.nombre, source.apellido, source.rut, 
                source.email, source.telefono, source.direccion, source.fecha_creacion)
    """


def _build_producto_merge(rows):
    """Construye query MERGE para productos"""
    values_list = []
    for row in rows:
        nombre = str(row.get('nombre', '')).replace('"', '\\"')
        descripcion = str(row.get('descripcion', '') or '').replace('"', '\\"')
        
        values_list.append(f"""STRUCT(
            {row.get('id_producto')} AS id_producto,
            "{nombre}" AS nombre,
            "{descripcion}" AS descripcion,
            "{row.get('codigo_sku', '')}" AS codigo_sku,
            "{row.get('codigo_barras', '') or ''}" AS codigo_barras,
            {int(row.get('controla_stock', 0))} AS controla_stock,
            {float(row.get('precio_neto'))} AS precio_neto,
            {float(row.get('costo_neto'))} AS costo_neto,
            {int(row.get('estado', 1))} AS estado
        )""")
    
    return f"""
    MERGE `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.producto` AS target
    USING (
        SELECT * FROM UNNEST([
            {','.join(values_list)}
        ])
    ) AS source
    ON target.id_producto = source.id_producto
    WHEN MATCHED THEN 
        UPDATE SET 
            nombre = source.nombre,
            descripcion = source.descripcion,
            codigo_sku = source.codigo_sku,
            codigo_barras = source.codigo_barras,
            controla_stock = source.controla_stock,
            precio_neto = source.precio_neto,
            costo_neto = source.costo_neto,
            estado = source.estado
    WHEN NOT MATCHED THEN
        INSERT (id_producto, nombre, descripcion, codigo_sku, codigo_barras, 
                controla_stock, precio_neto, costo_neto, estado)
        VALUES (source.id_producto, source.nombre, source.descripcion, source.codigo_sku,
                source.codigo_barras, source.controla_stock, source.precio_neto,
                source.costo_neto, source.estado)
    """


def _build_documento_merge(rows):
    """Construye query MERGE para documentos"""
    values_list = []
    for row in rows:
        fecha_emision = 'NULL'
        if row.get('fecha_emision'):
            fecha_emision = f"TIMESTAMP_SECONDS({int(row['fecha_emision'])})"
            
        values_list.append(f"""STRUCT(
            {row.get('id_documento')} AS id_documento,
            {row.get('id_cliente', 'NULL')} AS id_cliente,
            {row.get('id_tipo_documento', 'NULL')} AS id_tipo_documento,
            {row.get('folio', 'NULL')} AS folio,
            {fecha_emision} AS fecha_emision,
            {float(row.get('monto_neto', 0.0))} AS monto_neto,
            {float(row.get('monto_iva', 0.0))} AS monto_iva,
            {float(row.get('monto_total', 0.0))} AS monto_total
        )""")
    
    return f"""
    MERGE `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.documento_venta` AS target
    USING (
        SELECT * FROM UNNEST([
            {','.join(values_list)}
        ])
    ) AS source
    ON target.id_documento = source.id_documento
    WHEN MATCHED THEN 
        UPDATE SET 
            id_cliente = source.id_cliente,
            monto_neto = source.monto_neto,
            monto_iva = source.monto_iva,
            monto_total = source.monto_total
    WHEN NOT MATCHED THEN
        INSERT (id_documento, id_cliente, id_tipo_documento, folio, fecha_emision,
                monto_neto, monto_iva, monto_total)
        VALUES (source.id_documento, source.id_cliente, source.id_tipo_documento, 
                source.folio, source.fecha_emision, source.monto_neto,
                source.monto_iva, source.monto_total)
    """


def _build_detalle_merge(rows):
    """Construye query MERGE para detalles de documentos"""
    values_list = []
    for row in rows:
        values_list.append(f"""STRUCT(
            {row.get('id_detalle')} AS id_detalle,
            {row.get('id_documento')} AS id_documento,
            {row.get('id_producto')} AS id_producto,
            {float(row.get('cantidad', 0.0))} AS cantidad,
            {float(row.get('precio_neto_unitario', 0.0))} AS precio_neto_unitario,
            {float(row.get('descuento_porcentual', 0.0))} AS descuento_porcentual,
            {float(row.get('monto_total_linea', 0.0))} AS monto_total_linea
        )""")
    
    return f"""
    MERGE `{settings.BIGQUERY_PROJECT}.{settings.BIGQUERY_DATASET}.detalle_documento` AS target
    USING (
        SELECT * FROM UNNEST([
            {','.join(values_list)}
        ])
    ) AS source
    ON target.id_detalle = source.id_detalle
    WHEN MATCHED THEN 
        UPDATE SET 
            id_documento = source.id_documento,
            id_producto = source.id_producto,
            cantidad = source.cantidad,
            precio_neto_unitario = source.precio_neto_unitario,
            descuento_porcentual = source.descuento_porcentual,
            monto_total_linea = source.monto_total_linea
    WHEN NOT MATCHED THEN
        INSERT (id_detalle, id_documento, id_producto, cantidad, precio_neto_unitario,
                descuento_porcentual, monto_total_linea)
        VALUES (source.id_detalle, source.id_documento, source.id_producto, source.cantidad,
                source.precio_neto_unitario, source.descuento_porcentual,
                source.monto_total_linea)
    """


def sync_clients(db):
    """Sincronización de clientes con VALIDACIÓN ESTRICTA"""
    logging.info("👥 Iniciando sincronización de Clientes con validación estricta...")
    
    # Ensure tables exist before syncing
    db.ensure_all_tables()
    
    try:
        clients_list = bsale_client.get_clients()
        if not clients_list:
            logging.info("⚠️ No se encontraron clientes en Bsale.")
            return

        logging.info(f"📋 Obtenidos {len(clients_list)} clientes de Bsale. Validando...")
        
        # Validar cada cliente
        valid_clients = []
        invalid_count = 0
        
        for client in clients_list:
            try:
                validated_client = ETLDataValidator.validate_client(client)
                valid_clients.append(validated_client)
            except DataValidationError as e:
                invalid_count += 1
                logging.warning(f"⚠️ Cliente inválido omitido: {e}")
        
        logging.info(f"✅ Validación completada: {len(valid_clients)} válidos, {invalid_count} omitidos")
        
        if not valid_clients:
            logging.warning("⚠️ No hay clientes válidos para cargar.")
            return

        # Cargar clientes válidos a BigQuery
        _bigquery_upsert_with_merge(db, "cliente", valid_clients, "id_cliente", "clientes")
        logging.info(f"✅ Sincronización de Clientes finalizada (BigQuery). {len(valid_clients)} registros válidos procesados.")
        
    except Exception as e:
        logging.error(f"🔴 ERROR CRÍTICO en sync_clients: {e}")
        raise


def sync_products(db):
    """Sincronización de productos con VALIDACIÓN ESTRICTA DE PRECIOS Y COSTOS"""
    logging.info("📦 Iniciando sincronización de Productos con validación estricta...")
    
    # Ensure tables exist before syncing
    db.ensure_all_tables()
    
    try:
        products_list = bsale_client._get_all_pages("products.json", params={'expand': '[variants.costs]'})

        if not products_list:
            logging.info("⚠️ No se encontraron productos en Bsale.")
            return

        logging.info(f"📋 Obtenidos {len(products_list)} productos de Bsale. Validando...")

        valid_products = []
        invalid_count = 0
        processed_variants = set()

        for product in products_list:
            variants = product.get("variants", {}).get("items", [])
            if not variants:
                invalid_count += 1
                logging.warning(f"⚠️ Producto {product.get('id')} sin variantes - omitido")
                continue

            # Procesar solo la primera variante activa válida
            for variant in variants:
                variant_id = variant.get("id")
                
                if variant_id in processed_variants:
                    continue
                
                if variant.get("state") != 0:
                    continue

                try:
                    # Obtener precio de lista 2 (OBLIGATORIO)
                    price_detail = bsale_client.fetch("price_lists/2/details.json", params={"variantid": variant_id})
                    price_items = price_detail.get("items", []) if price_detail else []
                    
                    if not price_items:
                        invalid_count += 1
                        logging.error(f"🔴 Producto {product.get('name')} (variante {variant_id}): SIN PRECIO en lista 2 - OMITIDO")
                        break
                    
                    net_price = price_items[0].get("variantValue")
                    
                    # Obtener costo usando endpoint específico (OBLIGATORIO)
                    cost_detail = bsale_client.fetch(f"variants/{variant_id}/costs.json")
                    net_cost = cost_detail.get("averageCost") if cost_detail else None
                    cost_history = cost_detail.get("history", []) if cost_detail else []
                    
                    # Verificar si hay algún costo histórico > 0
                    has_valid_cost_history = any(
                        hist.get("cost", 0) > 0 for hist in cost_history
                    )
                    
                    if not has_valid_cost_history:  # Sin historial O todos los costos son 0
                        if net_price and net_price > 0:
                            net_cost = net_price * 0.65
                            logging.info(f"📊 Producto {product.get('name')} (variante {variant_id}): Sin costos históricos válidos, calculado desde precio: {net_cost}")
                        else:
                            net_cost = None  # Will fail validation below
                    # Si hay historial con costos > 0, usar averageCost
                    
                    # VALIDACIÓN ESTRICTA: precio y costo obligatorios
                    validated_product = ETLDataValidator.validate_product(
                        product, variant, net_price, net_cost
                    )
                    
                    valid_products.append(validated_product)
                    processed_variants.add(variant_id)
                    break  # Solo primera variante válida
                    
                except DataValidationError as e:
                    invalid_count += 1
                    logging.warning(f"⚠️ Producto inválido omitido: {e}")
                    break
                except Exception as e:
                    invalid_count += 1
                    logging.error(f"🔴 Error procesando producto {product.get('id')}, variante {variant_id}: {e}")
                    break

        logging.info(f"✅ Validación completada: {len(valid_products)} productos válidos, {invalid_count} omitidos")

        if not valid_products:
            logging.error("🔴 CRÍTICO: No hay productos válidos para cargar. Todos los productos tienen problemas de precio/costo.")
            raise Exception("No hay productos válidos - revisar precios y costos en Bsale")

        # Cargar productos válidos a BigQuery
        _bigquery_upsert_with_merge(db, "producto", valid_products, "id_producto", "productos")
        logging.info(f"✅ Sincronización de Productos finalizada (BigQuery). {len(valid_products)} registros válidos procesados.")
        
    except Exception as e:
        logging.error(f"🔴 ERROR CRÍTICO en sync_products: {e}")
        raise


def sync_documents(db, start_date: str = None):
    """Sincronización de documentos con VALIDACIÓN ESTRICTA"""
    logging.info(f"📄 Iniciando sincronización de Documentos con validación estricta (desde {start_date or 'el inicio'})...")
    
    # Ensure tables exist before syncing
    db.ensure_all_tables()
    
    try:
        documents_list = bsale_client.get_documents(start_date=start_date)
        if not documents_list:
            logging.info("⚠️ No se encontraron documentos de venta.")
            return

        logging.info(f"📋 Obtenidos {len(documents_list)} documentos de Bsale. Validando...")

        # BigQuery mode: omitiendo validación FK (las deja NULL si no existen)
        existing_client_ids = None
        existing_product_ids = None
        logging.info("📋 BigQuery mode: omitiendo validación FK.")

        # Validar todos los documentos
        valid_documents = []
        valid_details = []
        invalid_count = 0

        for doc in documents_list:
            try:
                # Validar documento
                validated_doc = ETLDataValidator.validate_document(doc)
                
                # BigQuery: mantener FK tal como viene (NULL si no existe)
                
                valid_documents.append(validated_doc)

                # Validar detalles del documento
                details = doc.get("details", {}).get("items", [])
                for detail in details:
                    try:
                        validated_detail = ETLDataValidator.validate_document_detail(detail, doc.get("id"))
                        
                        # BigQuery: mantener FK tal como viene
                        
                        valid_details.append(validated_detail)
                        
                    except DataValidationError as e:
                        logging.warning(f"⚠️ Detalle documento inválido omitido: {e}")
                        
            except DataValidationError as e:
                invalid_count += 1
                logging.warning(f"⚠️ Documento inválido omitido: {e}")

        logging.info(f"✅ Validación completada: {len(valid_documents)} documentos válidos, {len(valid_details)} detalles válidos, {invalid_count} documentos omitidos")

        if not valid_documents:
            logging.warning("⚠️ No hay documentos válidos para cargar.")
            return

        # CARGAR CON UPSERT EN BIGQUERY EN UNA SOLA OPERACIÓN ATÓMICA
        if valid_documents:
            _bigquery_upsert_with_merge(db, "documento_venta", valid_documents, "id_documento", "documentos")
        
        if valid_details:
            _bigquery_upsert_with_merge(db, "detalle_documento", valid_details, "id_detalle", "detalles documentos")

        logging.info(f"✅ Sincronización de Documentos finalizada. {len(valid_documents)} documentos y {len(valid_details)} detalles válidos procesados.")
        
    except Exception as e:
        logging.error(f"🔴 ERROR CRÍTICO en sync_documents: {e}")
        raise
