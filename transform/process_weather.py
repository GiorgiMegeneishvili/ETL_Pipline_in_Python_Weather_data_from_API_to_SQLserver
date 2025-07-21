import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

def process_weather_data(raw_data, city):
    if raw_data:
        try:
            weather_data = {
                'City': city,
                'Temperature_C': raw_data['main']['temp'],
                'Weather': raw_data['weather'][0]['description'],
                'Humidity_%': raw_data['main']['humidity'],
                'Wind_Speed_mps': raw_data['wind']['speed'],
                'Timestamp': pd.Timestamp.now()
            }
            logger.info(f"Weather data processed for {city}")
            ### ეს აკარგადაა სანახავი
            # weather_data = pd.DataFrame([weather_data])
            return weather_data
        except KeyError as e:
            logger.error(f"Key error: {e}")
            return None
    else:
        logger.warning(f"No data to process for {city}")
        return None
