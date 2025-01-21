from datetime import datetime, timedelta
from sqlalchemy.exc import SQLAlchemyError

class TimePeriodHelper:
    @staticmethod
    def calculate_minutes(start_date: str, end_date: str = None) -> int:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date:
            try:
                end = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(hours=23, minutes=59, seconds=00)
        else:
            end = datetime.now()
        sub = end - start
        return int(sub.total_seconds() // 60)

    @staticmethod
    def generate_timestamps(start_date: str, end_date: str = None) -> list:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date:
            try:
                end = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                end = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(hours=23, minutes=59, seconds=00)
        else:
            end = datetime.now()
        timestamps = []
        current = start
        while current < end:
            timestamps.append(current.strftime("%Y-%m-%d %H:%M:%S"))
            current += timedelta(minutes=1)
        return timestamps

    @staticmethod
    def insert_timestamps(session, model, timestamps: list):
        try:
            for ts in timestamps:
                record = model(Timestamp=ts)
                session.add(record)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            raise e
