from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import SQLAlchemyError
from models import Historicos, Simulacion, PLC

class DatabaseOperations:
    def __init__(self, session):
        self.session = session

    def insert(self, obj):
        try:
            self.session.add(obj)
            self.session.commit()
            generated_id = next(
            (getattr(obj, attr) for attr in dir(obj) if attr.startswith("id") and not attr.startswith("_")),
                None
            )

            if generated_id is None:
                raise AttributeError(f"No se encontró un atributo 'id' en el objeto {obj}.")

            return generated_id
        except ValueError as ve:
            self.session.rollback()
            print(f"Validation error: {ve}")
        except Exception as e:
            self.session.rollback()
            print(f"Error inserting {obj}: {e}")

    def update(self, obj, updates):
        try:
            for key, value in updates.items():
                setattr(obj, key, value)
            if hasattr(obj, "validate"):
                obj.validate()
            self.session.commit()
        except ValueError as ve:
            self.session.rollback()
            print(f"Validation error: {ve}")
        except Exception as e:
            self.session.rollback()
            print(f"Error updating {obj}: {e}")

    def _validate_foreign_key(self, model, id_value):
        """Verifica si una llave foránea existe en la tabla referenciada."""
        try:
            self.session.query(model).filter_by(id_metadata=id_value).one()
        except NoResultFound:
            raise ValueError(f"La llave foránea con ID {id_value} no existe en la tabla {model.__tablename__}.")    

    def insert_historicos_from_dataframe(self, session, timestamps, series_df, id_plc, id_simulacion):
        historicos = []
        n_minutes = len(timestamps)
        if len(series_df) != n_minutes:
            raise ValueError("El número de filas en el DataFrame no coincide con el número de timestamps.")

        for i, timestamp in enumerate(timestamps):
            velocidad = series_df.loc[i, 'Serie_1']
            temperatura = series_df.loc[i, 'Serie_2']
            anomalia = bool(series_df.loc[i, 'Anomaly'])
            
            historico = Historicos(
                id_plc=id_plc,
                timestamp=timestamp,
                velocidad=velocidad,
                temperatura=temperatura,
                id_simulacion=id_simulacion,
                anomalia=anomalia
            )
            historicos.append(historico)

        try:
            session.add_all(historicos)
            session.commit()
            print(f"Se insertaron {len(historicos)} registros en la tabla Historicos.")
        except Exception as e:
            session.rollback()
            print(f"Error al insertar registros: {e}")

    def insert_simulacion(self, session, next_id_simulacion, ids_metadata, tipo_simulacion):
        try:
            simulaciones = []
            for id_metadata in ids_metadata:
                new_simulacion = Simulacion(
                    id_simulacion=next_id_simulacion,
                    tipo_simulacion=tipo_simulacion,
                    id_metadata=id_metadata
                )
                simulaciones.append(new_simulacion)
            
            session.add_all(simulaciones)
            session.commit()
            print(f"Se insertaron {len(ids_metadata)} registros en la tabla Simulacion con id_simulacion: {next_id_simulacion}")
        except SQLAlchemyError as e:
            session.rollback()
            print(f"Error al insertar en Simulacion: {e}")
    
    def get_ids_plc(self, session):
        try:
            ids_plc = session.query(PLC.id_plc).all()
            return [id_plc[0] for id_plc in ids_plc]
        except Exception as e:
            print(f"Error al obtener los IDs de PLC: {e}")
            return []