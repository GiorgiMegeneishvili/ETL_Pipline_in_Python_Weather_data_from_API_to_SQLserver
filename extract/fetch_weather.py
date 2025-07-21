import requests
from utils.logger import get_logger
from utils.config import API_KEY

logger = get_logger(__name__)
def fetch_weather(city):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric'
    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.info(f"Successfully fetched weather for {city}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather data for {city}: {e}")
        return None
