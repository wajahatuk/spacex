import asyncio
import logging
import signal
from data_pipeline import DataPipeline

logging.basicConfig(level=logging.INFO)

POLLING_INTERVAL = 3600  # 1 hour

async def shutdown(signal, loop):
    logging.info(f"Received exit signal {signal.name}...")
    loop.stop()

async def main():
    pipeline = DataPipeline()

    consecutive_errors = 0
    while True:
        try:
            await pipeline.run()
            consecutive_errors = 0  # reset error count on success
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            consecutive_errors += 1

        # Sleep for a defined interval before polling again
        # Introduce exponential back-off in case of consecutive errors
        sleep_duration = POLLING_INTERVAL * (2 ** consecutive_errors)
        logging.info(f"Sleeping for {sleep_duration} seconds...")
        await asyncio.sleep(sleep_duration)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    for s in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(
            s,
            lambda s=s: asyncio.create_task(shutdown(s, loop))
        )

    try:
        loop.run_until_complete(main())
    finally:
        logging.info("Successfully shutdown the service.")
        loop.close()
