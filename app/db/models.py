# app/db/models.py
from sqlalchemy import Column, Integer, String, DECIMAL, DATETIME, ForeignKey
from .base import Base # Importamos la Base declarativa
from sqlalchemy.schema import UniqueConstraint

class Cliente(Base):
    __tablename__ = 'cliente'
    id_bsale = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(255))
    apellido = Column(String(255), nullable=True)
    rut = Column(String(15), nullable=True, index=True) # Agregamos un índice a rut, es bueno para búsquedas
    email = Column(String(255), nullable=True)
    telefono = Column(String(50), nullable=True)
    direccion = Column(String(500), nullable=True)
    fecha_creacion = Column(DATETIME, nullable=True)

class Producto(Base):
    __tablename__ = 'producto'
    id_bsale = Column(Integer, primary_key=True, index=True)
    nombre = Column(String(255), nullable=False)
    descripcion = Column(String(1000), nullable=True)
    codigo_sku = Column(String(100), nullable=True, index=True)
    codigo_barras = Column(String(100), nullable=True, index=True)
    controla_stock = Column(Integer, comment="1 para verdadero, 0 para falso")
    precio_neto = Column(DECIMAL(10, 2), nullable=True)
    costo_neto = Column(DECIMAL(10, 2), nullable=True)
    estado = Column(Integer, comment="1 para activo, 0 para inactivo")

class DocumentoVenta(Base):
    __tablename__ = 'documento_venta'
    id_bsale = Column(Integer, primary_key=True, index=True)
    # IMPORTANTE: El cliente puede ser nulo en una venta
    id_cliente = Column(Integer, ForeignKey('cliente.id_bsale'), nullable=True)
    id_tipo_documento = Column(Integer)
    folio = Column(Integer, index=True) # Agregamos un índice al folio
    fecha_emision = Column(DATETIME, index=True) # La fecha de emisión es obligatoria
    monto_neto = Column(DECIMAL(10, 2), nullable=True)
    monto_iva = Column(DECIMAL(10, 2), nullable=True)
    monto_total = Column(DECIMAL(10, 2), nullable=True)

class DetalleDocumento(Base):
    __tablename__ = 'detalle_documento'
    id_detalle = Column(Integer, primary_key=True, autoincrement=True)
    id_documento = Column(Integer, ForeignKey('documento_venta.id_bsale'), index=True)
    # IMPORTANTE: La variante podría ser nula
    id_producto = Column(Integer, ForeignKey('producto.id_bsale'), nullable=True)
    cantidad = Column(DECIMAL(10, 2))
    precio_neto_unitario = Column(DECIMAL(10, 2))
    # IMPORTANTE: El descuento puede no existir
    descuento_porcentual = Column(DECIMAL(5, 2), nullable=True)
    monto_total_linea = Column(DECIMAL(10, 2), nullable=True)

    # Clave única para que el UPSERT funcione correctamente en el detalle.
    # Un documento no debería tener el mismo producto más de una vez.
    __table_args__ = (UniqueConstraint('id_documento', 'id_producto', name='_id_documento_producto_uc'),)