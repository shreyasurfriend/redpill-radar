import os
import requests
import json
import logging

logger = logging.getLogger(__name__)

ANALYSE_CONTENT_PATH = "/api/v1/content"


class DataProcessorAPI:
    def __init__(self, base_url=None, api_key=None):
        self.base_url = (base_url or os.getenv("ANALYSE_API_URL", "http://localhost:8000")).rstrip("/")
        self.api_key = api_key or os.getenv("ANALYSE_API_KEY")
        self.ingest_url = f"{self.base_url}{ANALYSE_CONTENT_PATH}"

    def push_data(self, tweets):
        """
        Push scraped tweets to the analyse ingestion API one at a time.
        Input format:  [{"id": "12345", "text": "tweet text"}, ...]
        Sends as:      {"twitter_id": "12345", "content_text": "tweet text"}
        """
        if not self.base_url:
            logger.warning("ANALYSE_API_URL is not set. Skipping push.")
            logger.info(f"Payload to push:\n{json.dumps(tweets, indent=2)}")
            return False

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        logger.info(f"Pushing {len(tweets)} tweets to {self.ingest_url}")
        success_count = 0
        skip_count = 0
        fail_count = 0

        for tweet in tweets:
            body = {
                "twitter_id": str(tweet["id"]),
                "content_text": tweet["text"],
            }
            try:
                response = requests.post(self.ingest_url, json=body, headers=headers)
                response.raise_for_status()
                result = response.json()

                if result.get("status") == "success":
                    logger.info(f"Ingested tweet {tweet['id']} -> internal id {result.get('id')}")
                    success_count += 1
                else:
                    logger.warning(f"Tweet {tweet['id']} skipped: {result.get('message')}")
                    skip_count += 1

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to push tweet {tweet['id']}: {e}")
                fail_count += 1

        logger.info(
            f"Push complete: {success_count} ingested, {skip_count} skipped, {fail_count} failed"
        )
        return fail_count == 0
