# app/db/models.py
# Este archivo ahora solo contiene las definiciones de esquemas para BigQuery
# Ya no usamos SQLAlchemy porque todo va a BigQuery directamente

# Tabla: cliente
CLIENTE_SCHEMA = [
    {'name': 'id_bsale', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'nombre', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'apellido', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'rut', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'telefono', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'direccion', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'fecha_creacion', 'type': 'DATETIME', 'mode': 'NULLABLE'},
]

# Tabla: producto
PRODUCTO_SCHEMA = [
    {'name': 'id_bsale', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'nombre', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'descripcion', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'codigo_sku', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'codigo_barras', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'controla_stock', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'precio_neto', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'costo_neto', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'estado', 'type': 'INTEGER', 'mode': 'NULLABLE'},
]

# Tabla: documento_venta
DOCUMENTO_VENTA_SCHEMA = [
    {'name': 'id_bsale', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'id_cliente', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'id_tipo_documento', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'folio', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'fecha_emision', 'type': 'DATETIME', 'mode': 'NULLABLE'},
    {'name': 'monto_neto', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'monto_iva', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'monto_total', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
]

# Tabla: detalle_documento
DETALLE_DOCUMENTO_SCHEMA = [
    {'name': 'id_detalle', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'id_documento', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'id_producto', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'cantidad', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'precio_neto_unitario', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'descuento_porcentual', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
    {'name': 'monto_total_linea', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
]