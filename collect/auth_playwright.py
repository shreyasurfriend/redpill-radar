import asyncio
import json
import os
from pathlib import Path

from playwright.async_api import async_playwright
from dotenv import load_dotenv

# Load env from repo root (shared .env)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

COOKIES_FILE = 'cookies.json'

async def capture_cookies():
    username = os.getenv('TWITTER_USERNAME')
    password = os.getenv('TWITTER_PASSWORD')
    email = os.getenv('TWITTER_EMAIL')
    
    if not username or not password:
        print("Error: TWITTER_USERNAME and TWITTER_PASSWORD must be set in .env")
        return

    print(f"Launching browser to authenticate {username}...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        print("Navigating to login page...")
        await page.goto("https://x.com/login")
        
        try:
            # Wait for username field
            print("Entering username...")
            await page.wait_for_selector('input[autocomplete="username"]', timeout=10000)
            await page.fill('input[autocomplete="username"]', username)
            await page.keyboard.press("Enter")
            
            # Sometimes X asks for email/phone before password if it detects unusual activity
            try:
                # Give it a moment to transition
                await page.wait_for_timeout(2000)
                # Check if it's asking for phone/email instead of password
                if await page.locator('input[data-testid="ocfEnterTextTextInput"]').is_visible():
                     print("Unusual activity detected by X. Entering email...")
                     await page.fill('input[data-testid="ocfEnterTextTextInput"]', email)
                     await page.keyboard.press("Enter")
            except Exception:
                pass
            
            # Wait for password field
            print("Entering password...")
            await page.wait_for_selector('input[name="password"]', timeout=10000)
            await page.fill('input[name="password"]', password)
            await page.keyboard.press("Enter")
            
            print("Submitted credentials, waiting for home feed...")
            try:
                # Try to wait for the home timeline URL
                await page.wait_for_url("https://x.com/home", timeout=10000)
                print("Login successful automatically!")
                await page.wait_for_timeout(3000) # give it a moment to set all cookies
            except Exception:
                print("\n=======================================================")
                print("ACTION REQUIRED: X requested manual verification.")
                print("There might be a CAPTCHA, 2FA, or an 'unusual login' block.")
                print("Please look at the browser window and complete the steps.")
                print("Once you see your home feed, close the browser window.")
                print("=======================================================")
                # Wait for the user to close the browser manually
                try:
                    await page.wait_for_event("close", timeout=0)
                except Exception:
                    pass

        except Exception as e:
            print(f"Automated login flow interrupted: {e}")
            print("Please complete the login manually in the browser window.")
            try:
                await page.wait_for_event("close", timeout=0)
            except Exception:
                pass

        if not page.is_closed():
            await page.close()
            
        print("\nSaving cookies...")
        cookies = await context.cookies()
        
        with open(COOKIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, indent=4)
            
        print(f"Successfully saved {len(cookies)} cookies to {COOKIES_FILE}!")
        print("You can now run `python3 main.py` and the scraper will use these cookies.")

if __name__ == "__main__":
    asyncio.run(capture_cookies())
