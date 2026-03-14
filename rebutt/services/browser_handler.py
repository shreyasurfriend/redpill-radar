"""Browser automation handler for opening and viewing tweets."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class BrowserHandler:
    """Manages browser automation for viewing tweets using Playwright."""
    
    def __init__(self, headless: bool = False):
        """
        Initialize browser handler.
        
        Args:
            headless: Run browser in headless mode if True
        """
        self.headless = headless
        self.browser = None
        self.context = None
        self.page = None
    
    async def init(self):
        """Initialize and start the browser."""
        try:
            from playwright.async_api import async_playwright
            
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(headless=False)
            self.context = await self.browser.new_context()
            self.page = await self.context.new_page()
            logger.info("Browser initialized (visible window)")
        except ImportError:
            logger.error("playwright library not installed. Run: pip install playwright")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise
    
    async def open_tweet_in_browser(self, twitter_id: str) -> bool:
        """
        Open a tweet in the browser.
        
        Args:
            twitter_id: The Twitter/X post ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.page:
                logger.warning("Browser page not initialized. Initializing now.")
                await self.init()
            
            tweet_url = f"https://twitter.com/i/web/status/{twitter_id}"
            logger.info(f"Opening tweet in browser: {tweet_url}")
            
            await self.page.goto(tweet_url, wait_until="networkidle")
            logger.info(f"Tweet {twitter_id} opened in browser")
            return True
            
        except Exception as e:
            logger.error(f"Failed to open tweet {twitter_id} in browser: {e}")
            return False
    
    async def close(self):
        """Close the browser gracefully."""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            logger.info("Browser closed")
        except Exception as e:
            logger.error(f"Error closing browser: {e}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.init()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
