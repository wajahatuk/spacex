import json
from datetime import datetime, timezone
import asyncpg
import logging

logging.basicConfig(level=logging.INFO)

with open("config.json", "r") as f:
    DB_CONFIG = json.load(f)

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
            );
            	-- Create spacex_cores table
            CREATE TABLE IF NOT EXISTS spacex_cores (
                core_id SERIAL PRIMARY KEY,
                launch_id TEXT REFERENCES spacex_launches(id),
                core TEXT,
                legs BOOLEAN,
                flight INTEGER,
                reused BOOLEAN,
                landpad TEXT,
                gridfins BOOLEAN,
                landing_type TEXT,
                landing_attempt BOOLEAN,
                landing_success BOOLEAN
            );
            
            -- Create spacex_fairings table
            CREATE TABLE IF NOT EXISTS spacex_fairings (
                fairing_id SERIAL PRIMARY KEY,
                launch_id TEXT REFERENCES spacex_launches(id),
                reused BOOLEAN,
                recovered BOOLEAN,
                recovery_attempt BOOLEAN,
                ships TEXT
            );
            
            -- Create spacex_links table
            CREATE TABLE IF NOT EXISTS spacex_links (
                link_id SERIAL PRIMARY KEY,
                launch_id TEXT REFERENCES spacex_launches(id),
                patch_large TEXT,
                patch_small TEXT,
                flickr_small TEXT[],  -- Assuming arrays to store multiple links
                flickr_original TEXT[],
                reddit_media TEXT,
                reddit_launch TEXT,
                reddit_campaign TEXT,
                reddit_recovery TEXT,
                article TEXT,
                webcast TEXT,
                presskit TEXT,
                wikipedia TEXT,
                youtube_id TEXT
            );
            
            -- Create spacex_failures table
            CREATE TABLE IF NOT EXISTS spacex_failures (
                failure_id SERIAL PRIMARY KEY,
                launch_id TEXT REFERENCES spacex_launches(id),
                time INTEGER,
                reason TEXT,
                altitude INTEGER
            );
            """

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


            Other_inserts = """
        
                        INSERT INTO  spacex_cores (launch_id, core, legs, flight, reused, landpad, gridfins, landing_type, landing_attempt, landing_success)
                        SELECT 
                            id,
                            (cores->0->>'core')::VARCHAR AS core,  -- Extracting first core as an example; you might need a loop for multiple cores
                            (cores->0->>'legs')::BOOLEAN AS legs,
                            (cores->0->>'flight')::INTEGER AS flight,
                            (cores->0->>'reused')::BOOLEAN AS reused,
                            (cores->0->>'landpad')::VARCHAR AS landpad,
                            (cores->0->>'gridfins')::BOOLEAN AS gridfins,
                            (cores->0->>'landing_type')::VARCHAR AS landing_type,
                            (cores->0->>'landing_attempt')::BOOLEAN AS landing_attempt,
                            (cores->0->>'landing_success')::BOOLEAN AS landing_success
                        FROM spacex_launches
                        WHERE id NOT IN (SELECT launch_id FROM spacex_cores);
                        
                        INSERT INTO spacex_fairings (launch_id, reused, recovered, recovery_attempt, ships)
                        SELECT 
                            id,
                            (fairings->>'reused')::BOOLEAN AS reused,
                            (fairings->>'recovered')::BOOLEAN AS recovered,
                            (fairings->>'recovery_attempt')::BOOLEAN AS recovery_attempt,
                            fairings->>'ships' AS ships
                        FROM spacex_launches
                        WHERE id NOT IN (SELECT launch_id FROM spacex_fairings);
                    
                        INSERT INTO spacex_links (launch_id, patch_large, patch_small, flickr_small, flickr_original, reddit_media, reddit_launch, reddit_campaign, reddit_recovery, article, webcast, presskit, wikipedia, youtube_id)
                        SELECT 
                            id,
                            links->'patch'->>'large' AS patch_large,
                            links->'patch'->>'small' AS patch_small,
                            ARRAY(SELECT jsonb_array_elements_text(links->'flickr'->'small')) AS flickr_small,
                            ARRAY(SELECT jsonb_array_elements_text(links->'flickr'->'original')) AS flickr_original,
                            links->'reddit'->>'media' AS reddit_media,
                            links->'reddit'->>'launch' AS reddit_launch,
                            links->'reddit'->>'campaign' AS reddit_campaign,
                            links->'reddit'->>'recovery' AS reddit_recovery,
                            links->>'article' AS article,
                            links->>'webcast' AS webcast,
                            links->>'presskit' AS presskit,
                            links->>'wikipedia' AS wikipedia,
                            links->>'youtube_id' AS youtube_id
                        FROM spacex_launches
                        WHERE id NOT IN (SELECT launch_id FROM spacex_links);
                    
                        INSERT INTO  spacex_failures (launch_id, time, reason, altitude)
                        SELECT 
                            id,
                            (failures->0->>'time')::INTEGER AS time,  -- Extracting first failure as an example; you might need a loop for multiple failures
                            failures->0->>'reason' AS reason,
                            (failures->0->>'altitude')::INTEGER AS altitude
                        FROM spacex_launches
                        WHERE id NOT IN (SELECT launch_id FROM spacex_failures);
                            """

            await self.conn.execute(Other_inserts)
            logging.info("Data Inserted Successfully Into Normalize Tables")

        except Exception as e:
            print(f"Database error: {e}")
        finally:
            await self.conn.close()
