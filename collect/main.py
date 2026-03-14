import asyncio
import logging
from pathlib import Path

from dotenv import load_dotenv

from scraper import TwitterScraper
from api_client import DataProcessorAPI

# Load env from repo root (shared .env)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    # Define target content keywords
    search_keywords = [
        "manosphere",
        "incel",
        "women hating", # Note: Twitter search natively handles phrases with spaces
        "red pill"
    ]
    
    logger.info("Initializing TwitterScraper...")
    scraper = TwitterScraper()
    
    try:
        # Authenticate
        await scraper.login()
        
        # Scrape content
        # We limit to 5 per keyword for testing purposes
        results = await scraper.search_content(search_keywords, max_tweets=5)
        
        if not results:
            logger.warning("No tweets found matching the criteria.")
            return

        # Initialize API client and push data
        api_client = DataProcessorAPI()
        
        logger.info(f"Preparing to push {len(results)} formatted tweets...")
        success = api_client.push_data(results)
        
        if success:
            logger.info("Workflow completed successfully.")
        else:
            logger.error("Data processing push failed.")

    except Exception as e:
        logger.error(f"Workflow aborted due to error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
