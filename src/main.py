import pytest
from src.spacex_client import SpaceXClient

@pytest.mark.asyncio
async def test_fetch_data(mocker):

    # Mock the response from the SpaceX API
    async def mock_get(*args, **kwargs):
        class MockResponse:
            @staticmethod
            async def json():
                return [{"name": "Test Launch", "id": "1"}]

            @staticmethod
            def raise_for_status():
                pass

            status = 200

        return MockResponse()

    # Replace the actual API call with our mock response
    mocker.patch('aiohttp.ClientSession.get', side_effect=mock_get)

    client = SpaceXClient()
    data = await client.fetch_data()

    # Check if data is not None
    assert data is not None, "Data should not be None"
    assert len(data) == 1
    assert data[0]['name'] == 'Test Launch'
    assert data[0]['id'] == '1'
