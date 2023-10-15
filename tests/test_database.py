import pytest
from src.database_manager import DatabaseManager

MOCK_DATA = [
    {
        "name": "Test Launch",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "date_unix": 1641034800,
        "date_local": "2006-03-25T10:30:00+12:00",
        "date_precision": "hour",
        "upcoming": False,
        "net": True,
        "window": 7200,
        "rocket": "rocket_test",
        "success": True,
        "details": "Successful test launch.",
        "cores": [],
        "fairings": {},
        "links": {},
        "failures": [],
        "crew": [],
        "ships": [],
        "capsules": [],
        "payloads": [],
        "tbd": False,
        "id": "test_id",
    }
]
@pytest.mark.asyncio
async def test_connection():
    db_manager = DatabaseManager()
    await db_manager.connect()
    assert db_manager.conn is not None, "Database connection failed."
    await db_manager.close()

@pytest.mark.asyncio
async def test_insert_data():
    db_manager = DatabaseManager()
    await db_manager.connect()
    try:
        await db_manager.insert_data(MOCK_DATA)
        async with db_manager.conn.transaction():
            row = await db_manager.conn.fetchrow("SELECT name FROM spacex_launches WHERE id = 'test_id';")
        assert row["name"] == "Test Launch", "Data insertion test failed."
    finally:
        await db_manager.close()