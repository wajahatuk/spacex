## SpaceX Data Pipeline

This project aims to create a data pipeline for fetching SpaceX launch data, storing it into a database, and further normalizing the data into separate tables for better data structure and organization.

### Modules

1. **SpaceX Client (`spacex_client.py`):**
   A module that fetches data from the SpaceX API.
   
2. **Database Manager (`database_manager.py`):**
   Manages database connections and handles operations like creating tables, inserting data, and further normalizing the data into various tables.
   
3. **Data Pipeline (`datapipeline.py`):**
   Orchestrates the data pipeline flow, from fetching data from the SpaceX API to inserting and normalizing it into the database.

4. **Main Test (`main.py`):**
   Contains the primary test for the SpaceX client.

5. **Unit Tests (`test_spacex_client.py`):**
   Contains unit tests for the SpaceX client.

### Database Structure

In addition to the main `spacex_launches` table, the database also has normalized tables:

- `spacex_cores`: Contains detailed information about rocket cores.
- `spacex_fairings`: Contains details about rocket fairings.
- `spacex_links`: Links associated with each launch.
- `spacex_failures`: Information about any failures associated with a launch.

### Installation & Setup

**Install Dependencies:**
    
If you're using pip:

   ```bash
    pip install -r requirements.txt
  ```
Make sure to add config.json within the project:

   ```json
{
    "user": "your-usernam",
    "password": "your-password",
    "database": "your-database",
    "host": "your-host",
    "port": "your-port"
}
   ```

### Running the Pipeline

To run the pipeline:
```bash
python src/main.py
```

This script will:
- Fetch data from the SpaceX API
- Insert the fetched data into the `spacex_launches` table in the database
- Normalize the data and insert it into additional tables like `spacex_cores`, `spacex_fairings`, etc.

### Running Tests

To run tests:
```bash
pytest tests/
```

### Logging

The system uses Python's built-in logging. It logs basic INFO level messages, like successful data insertions and normalization status.