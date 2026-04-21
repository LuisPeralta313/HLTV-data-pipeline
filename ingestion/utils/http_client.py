"""
Playwright-based HTTP client that bypasses Cloudflare's JS challenge.
Returns the final rendered HTML after JavaScript execution.
"""

import time
import random
from contextlib import contextmanager
from typing import Optional

from loguru import logger
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext


# Realistic Chrome headers injected into every request
_EXTRA_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}


class HLTVClient:
    """
    Singleton-style context manager that keeps a single Playwright browser
    open for the lifetime of a scraping session. Reuses the browser across
    multiple page fetches to avoid repeated cold-start overhead.

    Usage:
        with HLTVClient() as client:
            html = client.get("https://www.hltv.org/results")
    """

    def __init__(
        self,
        headless: bool = True,
        min_delay: float = 3.0,
        max_delay: float = 6.0,
        timeout_ms: int = 30_000,
    ):
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.timeout_ms = timeout_ms

        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "HLTVClient":
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",  # hide webdriver flag
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        self._context = self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers=_EXTRA_HEADERS,
        )
        # Mask navigator.webdriver = true (Cloudflare checks this)
        self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("Playwright browser started (headless={})", self.headless)
        return self

    def __exit__(self, *_):
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
        logger.info("Playwright browser closed")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(
        self,
        url: str,
        wait_for: str = "domcontentloaded",
        wait_for_selector: Optional[str] = None,
    ) -> str:
        """
        Navigate to *url* and return the fully rendered HTML.

        Args:
            url:               Full URL to fetch.
            wait_for:          Playwright wait_until strategy.
                               "domcontentloaded" is used by default — it is
                               faster and sufficient when wait_for_selector is
                               provided to gate on actual content.
            wait_for_selector: CSS selector to wait for before calling
                               page.content(). Ensures JS-rendered content is
                               present in the DOM (e.g. 'div.result-con').
                               If None, page.content() is called immediately
                               after the page load event.

        Returns:
            HTML string of the page after JS execution.

        Raises:
            RuntimeError: if not used as a context manager.
        """
        if self._context is None:
            raise RuntimeError("HLTVClient must be used as a context manager")

        self._polite_delay()

        page: Page = self._context.new_page()
        try:
            logger.debug("GET {}", url)
            page.goto(url, wait_until=wait_for, timeout=self.timeout_ms)

            # If Cloudflare challenge is still running, wait for it to resolve
            if "Just a moment" in page.title():
                logger.warning("Cloudflare challenge detected — waiting up to 15 s …")
                page.wait_for_function(
                    "() => document.title !== 'Just a moment...'",
                    timeout=15_000,
                )

            if wait_for_selector:
                logger.debug("Waiting for selector '{}' …", wait_for_selector)
                page.wait_for_selector(wait_for_selector, timeout=15_000)

            html = page.content()
            logger.debug("Fetched {} bytes from {}", len(html), url)
            return html

        finally:
            page.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _polite_delay(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug("Sleeping {:.1f} s before next request", delay)
        time.sleep(delay)
