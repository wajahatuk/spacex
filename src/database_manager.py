import json
from datetime import datetime, timezone
import asyncpg
import logging

logging.basicConfig(level=logging.INFO)

DB_CONFIG = {
    "user": "uadmin",
    "password": "password123",
    "database": "spacex",
    "host": "localhost",
    "port": 5432
}

class DatabaseManager:
    def __init__(self):
        self.conn = None

    async def connect(self):
        self.conn = await asyncpg.connect(**DB_CONFIG)

    async def close(self):
        await self.conn.close()

    async def create_table(self):
        # Replace this with your CREATE_TABLE_QUERY
        query = """CREATE TABLE IF NOT EXISTS spacex_launches (
                name TEXT,
                date_utc TIMESTAMP WITH TIME ZONE,
                date_unix BIGINT,
                date_local TIMESTAMP WITHOUT TIME ZONE,
                date_precision TEXT,
                upcoming BOOLEAN,
                static_fire_date_utc TIMESTAMP,
                static_fire_date_unix BIGINT,
                net BOOLEAN,
                launch_window INTEGER,  -- renamed column from 'window' to 'launch_window'
                rocket_id TEXT,
                success BOOLEAN,
                details TEXT,
                cores JSONB,
                fairings JSONB,
                links JSONB,
                failures JSONB,
                crew JSONB,
                ships JSONB,
                capsules JSONB,
                payloads JSONB,
                tbd BOOLEAN,
                launch_library_id TEXT,
                id TEXT PRIMARY KEY,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            );"""
        await self.conn.execute(query)

    async def get_existing_ids(self):
        rows = await self.conn.fetch("SELECT id FROM spacex_launches")
        return {row["id"] for row in rows}

    async def insert_data(self, launches):
        INSERT_QUERY = """
                               INSERT INTO spacex_launches 
                               (name, date_utc, date_unix, date_local, date_precision, upcoming, static_fire_date_utc, static_fire_date_unix, net, launch_window, rocket_id, success, details, cores, fairings, links, failures, crew, ships, capsules, payloads, tbd, launch_library_id, id,created_at, updated_at)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26) 
                               ON CONFLICT (id) DO NOTHING;
                       """

        try:
            for launch in launches:

                static_fire_date_utc_dt = None
                if launch.get("static_fire_date_utc"):
                    static_fire_date_utc_dt = datetime.strptime(launch["static_fire_date_utc"],
                                                                '%Y-%m-%dT%H:%M:%S.%fZ').astimezone(
                        timezone.utc).replace(
                        tzinfo=None)

                await self.conn.execute(
                    INSERT_QUERY,
                    launch["name"],
                    datetime.strptime(launch["date_utc"], '%Y-%m-%dT%H:%M:%S.%fZ').astimezone(timezone.utc).replace(
                        tzinfo=None),
                    launch["date_unix"],
                    datetime.strptime(launch["date_local"], '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=None),
                    launch["date_precision"],
                    launch["upcoming"],
                    static_fire_date_utc_dt,
                    launch.get("static_fire_date_unix", None),
                    launch["net"],
                    launch["window"],
                    launch["rocket"],
                    launch["success"],
                    launch["details"],
                    json.dumps(launch.get("cores", [])),
                    json.dumps(launch.get("fairings", {})),
                    json.dumps(launch.get("links", {})),
                    json.dumps(launch.get("failures", [])),
                    json.dumps(launch.get("crew", [])),
                    json.dumps(launch.get("ships", [])),
                    json.dumps(launch.get("capsules", [])),
                    json.dumps(launch.get("payloads", [])),
                    launch["tbd"],
                    launch.get("launch_library_id"),
                    launch["id"],
                    datetime.now(),
                    datetime.now()
                )

            logging.info("Data Inserted Successfully")

        except Exception as e:
            print(f"Database error: {e}")

        finally:
            await self.conn.close()
