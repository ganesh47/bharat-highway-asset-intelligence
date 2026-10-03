#!/usr/bin/env python3
"""Test actual story data locally, or the published story with --url.

No dependencies/browsers are installed here. Local recovery uses one explicitly
corrupted calculation response; all normal scenarios use the approved bytes.
Deployed mode uses no response fixtures. Screenshots and diagnostics are retained.
"""
from __future__ import annotations

import argparse
import asyncio
import functools
import json
import threading
import traceback
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
CLAIM = 'CALC-ndcf_bridge'


class Handler(SimpleHTTPRequestHandler):
    extensions_map = {**SimpleHTTPRequestHandler.extensions_map, '.mjs': 'text/javascript'}

    def log_message(self, *_args):
        pass

    def translate_path(self, path):
        resolved = Path(super().translate_path(path))
        relative = resolved.relative_to(ROOT)
        if relative.parts and relative.parts[0] == 'packaged':
            remainder = Path(*relative.parts[1:])
            return str((ROOT if remainder.parts and remainder.parts[0] == 'data' else ROOT / 'apps/web') / remainder)
        return str(resolved)


async def run(args, url):
    from playwright.async_api import async_playwright

    results = []
    args.out.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as playwright:
        options = {'headless': True}
        if args.executable_path:
            options['executable_path'] = str(args.executable_path)
        browser = await playwright.chromium.launch(**options)
        try:
            root_url = url.replace('/apps/web/story.html','/story.html') if args.url else url.replace('/apps/web/story.html','/packaged/story.html')
            scenarios = ['desktop', 'mobile', 'deep-link'] + (['root-route'] if root_url != url else []) + ([] if args.url else ['checksum-retry','module-reload','module-late'])
            for name in scenarios:
                context = await browser.new_context(viewport={'width': 390 if name == 'mobile' else 1280, 'height': 900}, reduced_motion='reduce')
                page = await context.new_page()
                errors, failed, console, bad_http, calls = [], [], [], [], []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.on('requestfailed', lambda request: failed.append({'url': request.url, 'failure': request.failure}))
                page.on('console', lambda message: console.append(message.text) if message.type == 'error' else None)
                page.on('response', lambda response: bad_http.append({'url': response.url, 'status': response.status}) if response.status >= 400 else None)
                row = {'scenario': name, 'url': url, 'status': 'failed', 'page_errors': errors, 'request_failures': failed, 'console_errors': console, 'bad_http': bad_http}
                results.append(row)
                try:
                    if name in {'module-reload','module-late'}:
                        await page.add_init_script('const timer=window.setTimeout.bind(window);window.setTimeout=(fn,ms,...args)=>timer(fn,ms===20000?700:ms,...args);')
                        async def block_once(route):
                            calls.append(route.request.url)
                            if name=='module-late':
                                await asyncio.sleep(1.4)
                                await route.continue_()
                                return
                            if len(calls)==1:
                                await route.fulfill(status=200,body='await new Promise(()=>{});',content_type='text/javascript')
                            else:
                                await route.continue_()
                        await page.route('**/src/story.mjs',block_once)
                    if name == 'checksum-retry':
                        async def corrupt_once(route):
                            calls.append(route.request.url)
                            if len(calls) == 1:
                                # Other legitimate data requests finish first, so
                                # the checksum test needs no cancellation allowance.
                                await asyncio.sleep(.25)
                                await route.fulfill(status=200, body='{}', content_type='application/json')
                            else:
                                await route.continue_()
                        await page.route('**/calculations.v1.json', corrupt_once)
                    target = (root_url if name == 'root-route' else url) + ('#claim-' + CLAIM if name == 'deep-link' else '')
                    row['url'] = target
                    await page.goto(target, wait_until='commit' if name=='module-late' else 'networkidle', timeout=60000)
                    if name=='module-late':
                        await page.get_by_role('heading',name='Story viewer could not start').wait_for()
                        assert await page.get_by_role('button',name='Reload story').is_visible()
                    if name == 'module-reload':
                        await page.get_by_role('heading',name='Story viewer could not start').wait_for()
                        assert await page.locator('.exhibit-content table').count()==0
                        assert await page.locator('#story-exhibits').get_attribute('aria-busy')=='false'
                        await page.screenshot(path=str(args.out/'module-reload-failure.png'))
                        await page.get_by_role('button',name='Reload story').click()
                    if name == 'checksum-retry':
                        await page.get_by_role('heading', name='Story evidence could not be loaded').wait_for()
                        assert await page.locator('.exhibit-content table').count() == 0
                        assert 'checksum differs' in await page.locator('#story-status').inner_text()
                        assert await page.locator('body').get_attribute('data-story-ready') is None
                        await page.screenshot(path=str(args.out / (name + '-failure.png')))
                        await page.locator('#story-retry').evaluate('(button)=>{button.click();button.click();}')
                    await page.locator('body[data-story-ready="true"]').wait_for(timeout=40000)
                    assert await page.locator('.exhibit-content table').count() == 21
                    assert await page.locator('#evidence-catalog .source-record').count() == 548
                    assert '55 reproducible calculations' in await page.locator('#story-status').inner_text()
                    assert await page.locator('#story-exhibits').get_attribute('aria-busy') == 'false'
                    assert await page.locator('#story-retry').is_hidden()
                    assert await page.locator('#story-reload').is_hidden()
                    # Exact decimal, units, dates and classification in distinct scopes.
                    for identifier, text in [('NHAI-C148', '14,65,842.545'), (CLAIM, '2,234.2400'), ('NHIT-FY2026_DPU_return_of_capital', '0'), ('MISSING-AI_ROI', 'Unavailable')]:
                        anchor = page.locator('.exhibit-content a[data-observation="' + identifier + '"]')
                        assert await anchor.inner_text() == text
                        assert await anchor.get_attribute('href') == '#claim-' + identifier
                    assert '2026-06-30' in await page.locator('.exhibit-content a[data-observation="NHAI-C148"]').locator('..').inner_text()
                    assert 'ETC only' in await page.locator('#resilience .comparison-warning').inner_text()
                    assert 'Tender' in await page.locator('.exhibit-content a[data-observation="TECH-atms_scope"]').locator('..').inner_text()
                    if name == 'desktop':
                        await page.keyboard.press('Tab')
                        assert await page.locator('.skip-link').evaluate('(link)=>link===document.activeElement')
                        await page.keyboard.press('Enter')
                        assert await page.locator('#story-main').evaluate('(main)=>main===document.activeElement')
                        await page.locator('#distribution').screenshot(path=str(args.out / 'distribution-desktop.png'))
                    await page.screenshot(path=str(args.out / (name + '.png')))
                    if name == 'mobile':
                        assert await page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                        await page.locator('#monitoring').screenshot(path=str(args.out / 'monitoring-mobile.png'))
                        await page.add_style_tag(content='html {font-size: 200% !important}')
                        assert await page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                        # Wide semantic tables stay locally scrollable at enlarged text.
                        region = page.locator('#portfolio-scale .story-table-scroll').first
                        assert await region.evaluate('(node)=>node.scrollWidth > node.clientWidth')
                        await region.focus()
                        assert await region.evaluate('(node)=>node===document.activeElement')
                        await page.screenshot(path=str(args.out / 'mobile-200percent.png'))
                    if name in {'desktop', 'deep-link'}:
                        if name == 'desktop':
                            await page.locator('.exhibit-content a[data-observation="' + CLAIM + '"]').click()
                        detail = page.locator('#claim-' + CLAIM)
                        await page.wait_for_function('(id)=>document.getElementById(id).open',arg='claim-'+CLAIM)
                        assert await detail.get_attribute('open') is not None
                        assert await detail.locator('summary').evaluate('(summary)=>summary===document.activeElement')
                        assert await detail.locator('a[target="_blank"]').get_attribute('href') == 'https://nhit.co.in/pdf/annual-report/NHIT_Annual%20Report%20FY%202025-26.pdf'
                        assert await detail.locator('li a').count() == 4
                        await detail.screenshot(path=str(args.out / (name + '-claim.png')))
                        search = page.get_by_role('searchbox', name='Search observations')
                        await search.fill('MISSING-AI_ROI')
                        assert await page.locator('.source-record:visible').count() == 1
                        await page.locator('.exhibit-content a[data-observation="'+CLAIM+'"]').click()
                        assert await search.input_value()==''
                        assert await detail.get_attribute('open') is not None
                        await search.fill('MISSING-AI_ROI')
                        # A hash link must reveal its claim even after filtering hides it.
                        await page.evaluate("location.hash='#claim-TECH-atms_scope'")
                        await page.wait_for_function('document.getElementById("claim-TECH-atms_scope").open')
                        assert await search.input_value() == ''
                        assert await page.locator('#claim-TECH-atms_scope').get_attribute('open') is not None
                        assert 'Tender' in await page.locator('#claim-TECH-atms_scope').inner_text()
                    if name == 'checksum-retry':
                        assert len(calls) == 2, 'Double retry launched duplicate calculation reads'
                    if name == 'module-reload':
                        assert len(calls)==2, 'Reload did not request the actual story module'
                    await page.wait_for_load_state('networkidle')
                    assert not (errors or failed or console or bad_http), 'Unexpected browser diagnostics'
                    row['status'] = 'passed'
                except Exception as error:
                    row['error'] = repr(error)
                    row['traceback'] = traceback.format_exc()
                    await page.screenshot(path=str(args.out / (name + '-error.png')))
                finally:
                    await context.close()
                    # Late failures must revoke a passing result.
                    if errors or failed or console or bad_http:
                        row['status'] = 'failed'
        finally:
            await browser.close()
    report = {'generated_at': datetime.now(timezone.utc).isoformat(), 'mode': 'deployed-real-data' if args.url else 'local-real-data-and-controlled-recovery', 'results': results}
    (args.out / 'results.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    if any(row['status'] != 'passed' for row in results):
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=ROOT / 'buildcheck/story')
    parser.add_argument('--executable-path', type=Path)
    parser.add_argument('--url', help='Actual deployed story.html URL; disables fault injection')
    args = parser.parse_args()
    if args.url:
        parts = urlsplit(args.url)
        if parts.scheme not in {'https', 'http'} or parts.fragment or parts.query or not parts.path.endswith('/story.html'):
            parser.error('--url must be a story.html URL without a query or fragment')
        asyncio.run(run(args, args.url))
        return
    server = ThreadingHTTPServer(('127.0.0.1', 0), functools.partial(Handler, directory=str(ROOT)))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        asyncio.run(run(args, 'http://127.0.0.1:' + str(server.server_port) + '/apps/web/story.html'))
    finally:
        server.shutdown()
        server.server_close()
        worker.join()


if __name__ == '__main__':
    main()
