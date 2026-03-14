"""Main worker process for polling and processing harmful content."""

import asyncio
import logging
import signal
import sys
from typing import Optional

from config import settings
from services.rebuttal import RebuttalService
from services.browser_handler import BrowserHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RebuttalWorker:
    """Worker process for processing harmful content and generating rebuttals."""
    
    def __init__(self):
        """Initialize the worker."""
        self.running = False
        self.browser_handler: Optional[BrowserHandler] = None
        self.rebuttal_service: Optional[RebuttalService] = None
    
    async def initialize(self):
        """Initialize worker components."""
        try:
            # Initialize browser handler
            self.browser_handler = BrowserHandler(headless=settings.browser_headless)
            await self.browser_handler.init()
            logger.info("Browser handler initialized")
            
            # Services will be initialized per polling cycle
            logger.info("Worker initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize worker: {e}")
            return False
    
    async def run(self):
        """Run the main polling loop."""
        try:
            if not await self.initialize():
                logger.error("Failed to initialize worker. Exiting.")
                return
            
            self.running = True
            logger.info(f"Starting rebuttal worker (polling interval: {settings.rebuttal_polling_interval}s)")
            
            while self.running:
                try:
                    await self._poll_and_process()
                    await asyncio.sleep(settings.rebuttal_polling_interval)
                    
                except asyncio.CancelledError:
                    logger.info("Polling loop cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in polling loop: {e}")
                    # Continue polling on error
                    await asyncio.sleep(settings.rebuttal_polling_interval)
            
        finally:
            await self.shutdown()
    
    async def _poll_and_process(self):
        """Poll for unprocessed content and process it."""
        try:
            # Import here to avoid circular dependencies at module load time
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "analyse"))
            from app.database import async_session
            
            async with async_session() as session:
                rebuttal_service = RebuttalService(session)
                
                # Poll for unprocessed content
                items = await rebuttal_service.poll_for_unprocessed_content(limit=10)
                
                if not items:
                    logger.debug("No unprocessed content found")
                    return
                
                logger.info(f"Processing {len(items)} item(s)")
                
                # Process each item
                for item in items:
                    if not self.running:
                        break
                    
                    try:
                        success = await rebuttal_service.process_single_item(item)
                        
                        if success and self.browser_handler:
                            # Open tweet in browser for review
                            await self.browser_handler.open_tweet_in_browser(item.twitter_id)
                        
                    except Exception as e:
                        logger.error(f"Failed to process item {item.twitter_id}: {e}")
                        continue
        
        except Exception as e:
            logger.error(f"Error in polling cycle: {e}")
    
    async def shutdown(self):
        """Gracefully shutdown the worker."""
        logger.info("Shutting down worker...")
        self.running = False
        
        try:
            if self.browser_handler:
                await self.browser_handler.close()
                logger.info("Browser handler closed")
        except Exception as e:
            logger.error(f"Error closing browser handler: {e}")
        
        logger.info("Worker shutdown complete")
    
    def _handle_signal(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.running = False


async def main():
    """Main entry point."""
    worker = RebuttalWorker()
    
    # Register signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}. Starting graceful shutdown...")
        worker.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await worker.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        await worker.shutdown()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        await worker.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
