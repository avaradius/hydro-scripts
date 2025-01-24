from datetime import datetime, timedelta

class TimePeriodHelper:
    @staticmethod
    def calculate_minutes(start_date: str, end_date: str = None) -> int:
        start = TimePeriodHelper.parse_date(start_date, "00:00:00")
        end = TimePeriodHelper.parse_date(end_date, "23:59:00")

        sub = end - start
        print(f"minutes: '{end_date, int(sub.total_seconds() // 60)}'")
        return int(sub.total_seconds() // 60)

    @staticmethod
    def generate_timestamps(start_date: str, end_date: str = None) -> list:
        start = TimePeriodHelper.parse_date(start_date, "00:00:00")
        end = TimePeriodHelper.parse_date(end_date, "23:59:00")

        timestamps = []
        current = start
        while current < end:
            timestamps.append(current.strftime("%Y-%m-%d %H:%M:%S"))
            current += timedelta(minutes=1)
        print(f"timestamps: '{end_date, len(timestamps)}'")
        return timestamps

    @staticmethod
    def parse_date(date_str: str, default_time: str = "00:00:00") -> datetime:
        if not date_str or isinstance(date_str, float):
            current_time = datetime.now().strftime("%Y-%m-%d")
            date_obj = datetime.strptime(f"{current_time} {default_time}", "%Y-%m-%d %H:%M:%S")
        else:
            try:
                date_str = date_str.replace("/", "-")
                if len(date_str.split()) == 2:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                else:
                    date_obj = datetime.strptime(f"{date_str} {default_time}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    reordered_date = TimePeriodHelper.reorder_date_dd_mm_yyyy(date_str)
                    if len(reordered_date.split()) == 2:
                        date_obj = datetime.strptime(reordered_date, "%Y-%m-%d %H:%M:%S")
                    else:
                        date_obj = datetime.strptime(f"{reordered_date} {default_time}", "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    raise ValueError(f"Error al procesar la fecha '{date_str}': {e}")

        return date_obj.replace(second=0)

    @staticmethod
    def reorder_date_dd_mm_yyyy(date_str: str) -> str:
        try:
            day, month, year = date_str.split("-")
            return f"{year}-{month}-{day}"
        except Exception:
            raise ValueError(f"Fecha '{date_str}' no tiene el formato esperado DD-MM-YYYY.")