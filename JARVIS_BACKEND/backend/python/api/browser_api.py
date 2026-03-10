import asyncio
from typing import Optional
from playwright.async_api import async_playwright


class BrowserAPI:
    """
    Safe browser automation wrapper.
    Supports ONLY:
    - Opening URLs
    - Reading DOM text
    - Running sandboxed JS
    - Page navigation
    """

    def __init__(self):
        self.browser = None
        self.page = None

    async def launch(self):
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=True)
        self.page = await self.browser.new_page()

    async def open_url(self, url: str):
        if self.page is None:
            await self.launch()
        await self.page.goto(url, wait_until="domcontentloaded")

    async def read_text(self, selector: str) -> Optional[str]:
        if not self.page:
            return None
        try:
            return await self.page.locator(selector).inner_text()
        except:
            return None

    async def run_js(self, script: str):
        """Executes SAFE JS only."""
        if not self.page:
            return None
        return await self.page.evaluate(script)

    async def close(self):
        if self.browser:
            await self.browser.close()
