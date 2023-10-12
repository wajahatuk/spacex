from spacex_client import SpaceXClient
from database_manager import DatabaseManager

class DataPipeline:
    def __init__(self):
        self.client = SpaceXClient()
        self.db_manager = DatabaseManager()

    async def run(self):
        launches = await self.client.fetch_data()

        await self.db_manager.connect()
        await self.db_manager.create_table()
        await self.db_manager.insert_data(launches)
        await self.db_manager.close()