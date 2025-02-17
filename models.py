from sqlalchemy import Column, Integer, String, Float, Boolean, Text, TIMESTAMP, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates, relationship

Base = declarative_base()

# Modelo: Config
class Config(Base):
    __tablename__ = "config"
    id_metadata = Column(Integer, primary_key=True)
    timestamp = Column(TIMESTAMP, nullable=False)
    tipo_simulacion = Column(String(255), nullable=True)
    seed = Column(Integer, nullable=True)
    config = Column(Text, nullable=True)

    @validates("timestamp")
    def validate_timestamp(self, key, value):
        if not value:
            raise ValueError("El campo 'timestamp' no puede estar vacío.")
        return value


# Modelo: PLC
class PLC(Base):
    __tablename__ = "plc"
    id_plc = Column(Integer, primary_key=True)
    nombre_plc = Column(String(255), nullable=False)
    ubicacion = Column(Text, nullable=False)

    @validates("nombre_plc")
    def validate_nombre_plc(self, key, value):
        if not value or len(value) > 255:
            raise ValueError("El campo 'nombre_plc' es obligatorio y debe tener menos de 255 caracteres.")
        return value

    @validates("ubicacion")
    def validate_ubicacion(self, key, value):
        if not value:
            raise ValueError("El campo 'ubicacion' es obligatorio.")
        return value


# Modelo: Historicos
class Historicos(Base):
    __tablename__ = "historicos"
    id_historico = Column(Integer, primary_key=True)
    id_plc = Column(Integer, ForeignKey("plc.id_plc"), nullable=True)
    timestamp = Column(TIMESTAMP, nullable=False)
    velocidad = Column(Float, nullable=False)
    temperatura = Column(Float, nullable=False)
    id_metadata = Column(String(20), nullable=True)
    id_simulacion = Column(Integer, ForeignKey("simulacion.id_simulacion"), nullable=True)
    anomalia = Column(Boolean, nullable=False, default=False)

    plc = relationship("PLC")
    simulacion = relationship("Simulacion")

    @validates("timestamp")
    def validate_timestamp(self, key, value):
        if not value:
            raise ValueError("El campo 'timestamp' no puede estar vacío.")
        return value

    @validates("velocidad")
    def validate_velocidad(self, key, value):
        if value is None or value < 0:
            raise ValueError("El campo 'velocidad' debe ser un número positivo.")
        return value

    @validates("temperatura")
    def validate_temperatura(self, key, value):
        if value is None or value < 0:
            raise ValueError("El campo 'temperatura' debe ser un número positivo.")
        return value

    @validates("anomalia")
    def validate_anomalia(self, key, value):
        if not isinstance(value, bool):
            raise ValueError("El campo 'anomalia' debe ser un valor booleano.")
        return value

# Modelo: Historicos_Testing
class HistoricosTesting(Base):
    __tablename__ = "historicos_test"
    id_historico = Column(Integer, primary_key=True)
    id_plc = Column(Integer, ForeignKey("plc.id_plc"), nullable=True)
    timestamp = Column(TIMESTAMP, nullable=False)
    velocidad = Column(Float, nullable=False)
    temperatura = Column(Float, nullable=False)
    id_metadata = Column(String(20), nullable=True)
    id_simulacion = Column(Integer, ForeignKey("simulacion.id_simulacion"), nullable=True)
    anomalia = Column(Boolean, nullable=False, default=False)

    plc = relationship("PLC")
    simulacion = relationship("Simulacion")

    @validates("timestamp")
    def validate_timestamp(self, key, value):
        if not value:
            raise ValueError("El campo 'timestamp' no puede estar vacío.")
        return value

    @validates("velocidad")
    def validate_velocidad(self, key, value):
        if value is None or value < 0:
            raise ValueError("El campo 'velocidad' debe ser un número positivo.")
        return value

    @validates("temperatura")
    def validate_temperatura(self, key, value):
        if value is None or value < 0:
            raise ValueError("El campo 'temperatura' debe ser un número positivo.")
        return value

    @validates("anomalia")
    def validate_anomalia(self, key, value):
        if not isinstance(value, bool):
            raise ValueError("El campo 'anomalia' debe ser un valor booleano.")
        return value

# Modelo: Monitoreo_VW
class MonitoreoVW(Base):
    __tablename__ = "monitoreo_vw"
    id_monitoreo_vw = Column(Integer, primary_key=True)
    id_plc = Column(Integer, ForeignKey("plc.id_plc"), nullable=True)
    timestamp = Column(TIMESTAMP, nullable=False)
    velocidad = Column(Float, nullable=False)
    temperatura = Column(Float, nullable=False)
    id_metadata = Column(String(20), nullable=True)
    id_simulacion = Column(Integer, ForeignKey("simulacion.id_simulacion"), nullable=True)

    plc = relationship("PLC")
    simulacion = relationship("Simulacion")

    @validates("timestamp")
    def validate_timestamp(self, key, value):
        if not value:
            raise ValueError("El campo 'timestamp' no puede estar vacío.")
        return value

    @validates("velocidad")
    def validate_velocidad(self, key, value):
        if value is None or value < 0:
            raise ValueError("El campo 'velocidad' debe ser un número positivo.")
        return value

    @validates("temperatura")
    def validate_temperatura(self, key, value):
        if value is None or value < 0:
            raise ValueError("El campo 'temperatura' debe ser un número positivo.")
        return value

# Modelo: Simulacion
class Simulacion(Base):
    __tablename__ = "simulacion"
    id_simulacion = Column(Integer, primary_key=True)
    tipo_simulacion = Column(String(255), nullable=True)
    id_metadata = Column(Integer, ForeignKey("config.id_metadata"), primary_key=True)
    table_name = Column(String(50), nullable=True)

    config = relationship("Config")

    @validates("id_metadata")
    def validate_id_metadata(self, key, value):
        if not value:
            raise ValueError("El campo 'id_metadata' es obligatorio.")
        return value

    @validates("tipo_simulacion")
    def validate_tipo_simulacion(self, key, value):
        if value and len(value) > 255:
            raise ValueError("El campo 'tipo_simulacion' debe tener menos de 255 caracteres.")
        return value
