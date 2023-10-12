import aiohttp
import logging
logging.basicConfig(level=logging.INFO)


API_URL = "https://api.spacexdata.com/v5/launches/"

class SpaceXClient:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def fetch_data(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(API_URL) as response:
                    response.raise_for_status()
                    return await response.json()
        except aiohttp.ClientResponseError as cre:
            self.logger.error(f"HTTP error occurred: {cre}")
        except aiohttp.ClientError as ce:
            self.logger.error(f"Network or connection error occurred: {ce}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while fetching data: {e}")

        return []
