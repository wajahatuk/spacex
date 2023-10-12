import pytest
from src.spacex_client import SpaceXClient

@pytest.mark.asyncio
async def test_fetch_data(mocker):
    # Mock the response from the SpaceX API
    def mock_get(*args, **kwargs):
        class MockResponse:
            @staticmethod
            async def json():
                return [{"name": "Test Launch", "id": "1"}]

            @staticmethod
            def raise_for_status():
                pass

            status = 200

        # This mock mimics the async context manager behavior
        class MockContextManager:
            async def __aenter__(self):
                return MockResponse()

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

        return MockContextManager()

    # Replace the actual API call with our mock response
    mocker.patch('aiohttp.ClientSession.get', return_value=mock_get())

    client = SpaceXClient()
    data = await client.fetch_data()
    assert len(data) == 1
    assert data[0]['name'] == 'Test Launch'
    assert data[0]['id'] == '1'
