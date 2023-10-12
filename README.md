## SpaceX Data Pipeline

This project aims to create a data pipeline for fetching SpaceX launch data and storing it into a database.

### Modules

1. **SpaceX Client (`spacex_client.py`):**
   A module that fetches data from the SpaceX API.
   
2. **Database Manager (`database_manager.py`):**
   Manages database connections and handles operations like creating tables and inserting data.
   
3. **Data Pipeline (`datapipeline.py`):**
   Orchestrates the data pipeline flow, from fetching data from the SpaceX API to inserting it into the database.

4. **Main Test (`main.py`):**
   Contains the primary test for the SpaceX client.

5. **Unit Tests (`test_spacex_client.py`):**
   Contains unit tests for the SpaceX client.

### Installation & Setup

**Install Dependencies:**

    If you're using pip:
    ```bash
    pip install -r requirements.txt
    ```

### Running the Pipeline

To run the pipeline:
```bash
python src/main.py
```

### Running Tests

To run tests:
```bash
pytest tests/
```

### Logging

The system uses Python's built-in logging. It logs basic INFO level messages, like successful data insertions.