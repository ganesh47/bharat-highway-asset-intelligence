#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio

from typing import Union


async def run_smoke(url: str, generate_screenshot: bool = True) -> int:
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:  # pragma: no cover - environment dependent
        print(f"Playwright not available: {exc}")
        return 1

    console_errors: list[str] = []
    failed_requests: list[str] = []
    failed_responses: list[str] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        page.on("console", lambda message: _handle_console(message, console_errors))
        page.on(
            "requestfailed",
            lambda request: _on_request_failed(request, failed_requests),
        )

        async def on_response(response):
            try:
                if response.status >= 400:
                    failed_responses.append(f"{response.url} -> {response.status}")
            except Exception:
                pass

        page.on("response", lambda response: asyncio.create_task(on_response(response)))

        try:
            await page.goto(url, wait_until="networkidle", timeout=120000)
            await page.wait_for_timeout(3000)
            header = (await page.locator("h1").first.text_content()) or ""
            if "Bharat Highway Evidence Console" not in header:
                print("Unexpected header text:", header.strip())
                await browser.close()
                return 1

            if await page.locator("text=Could not load catalog").count():
                print("Catalog load error message found.")
                await browser.close()
                return 1

            if await page.locator("text=DuckDB initialization failed").count():
                print("DuckDB init error message found.")
                await browser.close()
                return 1

            chart_count = await page.locator(".metric-card").count()
            if chart_count < 3:
                print(f"Expected multiple metric cards, found {chart_count}.")
                await browser.close()
                return 1

            summary_text = await page.locator(".summary .card").all_text_contents()
            if not summary_text:
                print("Summary cards not found.")
                await browser.close()
                return 1

            if generate_screenshot:
                await page.screenshot(path="buildcheck/last-smoke.png", full_page=True)
            await browser.close()
        except Exception as exc:
            print(f"Playwright interaction failed: {exc}")
            await browser.close()
            return 1

    errors = [c for c in console_errors if "404" not in c and "Failed to load resource" not in c]
    if failed_requests:
        print("Request failures detected:")
        for item in failed_requests:
            print("-", item)
        return 1

    if failed_responses:
        print("HTTP error responses detected:")
        for item in failed_responses:
            print("-", item)
        return 1

    if errors:
        print("Console errors detected:")
        for item in errors:
            print("-", item)
        return 1

    print(f"Playwright smoke passed. URL={url}")
    return 0


def _handle_console(message, console_errors):
    if message.type in {"error", "warning"}:
        text = message.text
        if text:
            console_errors.append(text)


def _on_request_failed(request, failed_requests: list[str]):
    failure: Union[None, str, object] = request.failure
    if failure is None:
        reason = "unknown"
    elif isinstance(failure, str):
        reason = failure
    else:
        reason = getattr(failure, "error_text", None) or getattr(failure, "errorText", None) or str(failure)
    failed_requests.append(f"{request.url} ({reason})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a browser smoke check for the deployed dashboard.")
    parser.add_argument("--url", default="https://ganesh47.github.io/bharat-highway-asset-intelligence/apps/web/")
    parser.add_argument("--no-generate-screenshot", action="store_true")
    args = parser.parse_args()

    code = asyncio.run(run_smoke(args.url, generate_screenshot=not args.no_generate_screenshot))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
