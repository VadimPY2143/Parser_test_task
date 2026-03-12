import asyncio
import os
import re
from typing import Iterable
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
import httpx
from playwright.async_api import async_playwright

from app.models import HotlineOffer

_PRICE_RE = re.compile(r"([0-9][0-9\s]+)\s*(₴|грн\.?|uah)", re.IGNORECASE)
_NUMBER_RE = re.compile(r"\b[0-9][0-9\s]{2,}\b")


def normalize_hotline_url(raw_url: str) -> str:
    parts = urlsplit(raw_url)
    path = parts.path or ""

    for lang in ("ua", "ru", "uk"):
        prefix = f"/{lang}/"
        if path.startswith(prefix):
            path = path[len(prefix) - 1 :]
            break

    if path.endswith("/"):
        path = path[:-1]

    return f"{parts.scheme}://{parts.netloc}{path}"


async def fetch_offers(url: str) -> list[HotlineOffer]:
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
    page_title: str | None = None

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/26.3 Safari/605.1.15"
            ),
            locale="uk-UA",
        )
        page = await context.new_page()

        try:
            page.set_default_timeout(60000)
            await page.goto(url, wait_until="domcontentloaded")
            await _open_offers_tab(page)
            await _wait_for_offers(page, max_wait_ms=30000)
            page_title = await _get_page_title(page)
            dom_offers = await _extract_offers_dom(page, page_title)
            if dom_offers:
                return await _enrich_offers(dom_offers, page_title)
            html = await page.content()
        finally:
            await context.close()
            await browser.close()

    fallback_offers = _extract_offers(html, base_url="https://hotline.ua")
    return await _enrich_offers(fallback_offers, page_title)


async def _extract_offers_dom(page, page_title: str | None) -> list[HotlineOffer]:
    items = await page.locator("text=Купити").element_handles()
    if not items:
        items = await page.locator("text=КУПИТИ").element_handles()
    if not items:
        return []

    offers: list[HotlineOffer] = []
    seen: set[tuple[str | None, int | None, str | None]] = set()
    for handle in items:
        container = await handle.evaluate_handle(
            """
            (el) => {
              const hasPrice = (node) => {
                if (!node) return false;
                if (node.querySelector('[itemprop="price"]')) return true;
                if (node.querySelector('[data-price], [data-price-uah], [data-price-value], [data-priceuah]')) return true;
                if (node.querySelector('[class*="price"]')) return true;
                return false;
              };
              let cur = el;
              for (let i = 0; i < 10; i++) {
                if (!cur) break;
                if (hasPrice(cur)) return cur;
                cur = cur.parentElement;
              }
              return el.closest('article, li, div') || el.parentElement;
            }
            """
        )
        data = await container.evaluate(
            """
            (el) => {
              const text = el.innerText || '';
              const links = Array.from(el.querySelectorAll('a'))
                .map(a => ({text: a.innerText?.trim() || '', href: a.getAttribute('href') || ''}));
              const priceNodes = Array.from(
                el.querySelectorAll('[itemprop=\"price\"], [data-price], [data-price-uah], [data-price-value], [data-priceuah], [class*=\"price\"]')
              );
              const priceText = priceNodes.map(n => {
                return [
                  n.getAttribute('content'),
                  n.getAttribute('data-price'),
                  n.getAttribute('data-price-uah'),
                  n.getAttribute('data-price-value'),
                  n.getAttribute('data-priceuah'),
                  n.textContent
                ].filter(Boolean).join(' ');
              }).join(' ');
              return { text, links, priceText };
            }
            """
        )

        text = _normalize_text(data.get("text", ""))
        price = _extract_price_from_text(
            _normalize_text(data.get("priceText", "")) or text
        )
        links = data.get("links", [])
        shop = _pick_shop_from_links(links)
        offer_url = _pick_offer_url_from_links(links)
        original_url = _pick_original_url_from_links(data.get("links", []))
        title = None
        is_used = _detect_used(text)

        key = (shop, price, offer_url)
        if key in seen:
            continue
        seen.add(key)

        offers.append(
            HotlineOffer(
                url=_normalize_href(offer_url, "https://hotline.ua") if offer_url else None,
                original_url=_normalize_href(original_url, "https://hotline.ua")
                if original_url
                else None,
                title=title or page_title,
                shop=shop,
                price=price,
                is_used=is_used,
            )
        )

    return offers


async def get_offers(
    url: str,
    timeout_limit: int | None,
    price_sort: str | None,
    count_limit: int | None,
) -> tuple[str, list[HotlineOffer]]:
    clean_url = normalize_hotline_url(url)

    if timeout_limit is not None:
        offers = await asyncio.wait_for(fetch_offers(url), timeout=timeout_limit)
    else:
        offers = await fetch_offers(url)

    offers = _filter_valid_offers(offers)
    offers = _apply_sorting(offers, price_sort)
    offers = _apply_limit(offers, count_limit)

    return clean_url, offers


def _apply_sorting(offers: list[HotlineOffer], price_sort: str | None) -> list[HotlineOffer]:
    if price_sort is None:
        return offers
    key = lambda item: item.price if item.price is not None else 0
    reverse = price_sort.lower() == "desc"
    return sorted(offers, key=key, reverse=reverse)


def _apply_limit(offers: list[HotlineOffer], count_limit: int | None) -> list[HotlineOffer]:
    if count_limit is None or count_limit <= 0:
        return offers
    return offers[:count_limit]


def _filter_valid_offers(offers: list[HotlineOffer]) -> list[HotlineOffer]:
    filtered: list[HotlineOffer] = []
    for offer in offers:
        if not offer.url or not offer.shop or offer.price is None:
            continue
        filtered.append(offer)
    return filtered


async def _open_offers_tab(page) -> None:
    candidates = [
        page.get_by_role("tab", name=re.compile("де купити", re.IGNORECASE)),
        page.get_by_text("Де купити", exact=False),
        page.get_by_text("ДЕ КУПИТИ", exact=False),
    ]

    for locator in candidates:
        if await locator.count() == 0:
            continue
        handle = await locator.first.element_handle()
        try:
            await locator.first.scroll_into_view_if_needed()
            await locator.first.click(force=True, timeout=5000)
            return
        except Exception:
            if handle is not None:
                try:
                    await page.evaluate("(el) => el.click()", handle)
                    return
                except Exception:
                    pass

    await page.evaluate(
        """
        () => {
          const el = [...document.querySelectorAll('*')].find(e => {
            const t = e.textContent?.trim().toLowerCase();
            return t === 'де купити';
          });
          if (el) el.click();
        }
        """
    )


async def _wait_for_offers(page, max_wait_ms: int) -> None:
    step_ms = 1000
    waited = 0
    while waited < max_wait_ms:
        if await page.locator("text=Купити").count() > 0:
            return
        if await page.locator("text=₴").count() > 0:
            return
        if await page.locator("text=грн").count() > 0:
            return
        if await page.locator('a[href*="/go/price/"]').count() > 0:
            return
        await page.wait_for_timeout(step_ms)
        waited += step_ms


async def _get_page_title(page) -> str | None:
    try:
        h1 = await page.locator("h1").first.inner_text()
        title = _normalize_text(h1)
        if title:
            return title
    except Exception:
        pass
    try:
        title = _normalize_text(await page.title())
        return title or None
    except Exception:
        return None


async def _enrich_offers(
    offers: list[HotlineOffer],
    page_title: str | None,
) -> list[HotlineOffer]:
    for offer in offers:
        if not offer.title:
            offer.title = page_title or ""

    to_resolve = [offer for offer in offers if offer.url and not offer.original_url]
    if not to_resolve:
        return offers

    timeout = httpx.Timeout(10.0)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/26.3 Safari/605.1.15"
        )
    }

    semaphore = asyncio.Semaphore(5)

    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=timeout,
        headers=headers,
    ) as client:
        async def resolve(offer: HotlineOffer) -> None:
            async with semaphore:
                try:
                    resp = await client.get(offer.url)
                    final_url = str(resp.url) if resp.url else None
                    if final_url:
                        offer.original_url = final_url
                except Exception:
                    return

        await asyncio.gather(*(resolve(offer) for offer in to_resolve))

    for offer in offers:
        if offer.url and not offer.original_url:
            offer.original_url = offer.url
    return offers


def _extract_offers(html: str, base_url: str) -> list[HotlineOffer]:
    soup = BeautifulSoup(html, "html.parser")
    buy_buttons = soup.find_all(
        lambda tag: tag.name in {"a", "button"}
        and "купити" in tag.get_text(" ", strip=True).lower()
    )

    offers: list[HotlineOffer] = []
    seen: set[tuple[str | None, int | None, str | None]] = set()
    for button in buy_buttons:
        container = _find_offer_container(button)
        if container is None:
            continue
        text = _normalize_text(container.get_text(" ", strip=True))
        price = _extract_price(container, text)
        shop = _parse_shop(container)
        offer_url = _extract_href(button, base_url)
        original_url = _extract_original_url(container, base_url)
        title = _extract_title(container)
        is_used = _detect_used(text)

        key = (shop, price, offer_url)
        if key in seen:
            continue
        seen.add(key)

        offers.append(
            HotlineOffer(
                url=offer_url,
                original_url=original_url,
                title=title,
                shop=shop,
                price=price,
                is_used=is_used,
            )
        )

    return offers


def _find_offer_container(element, max_depth: int = 6):
    current = element
    for _ in range(max_depth):
        if current is None:
            break
        text = current.get_text(" ", strip=True)
        if _PRICE_RE.search(text):
            return current
        current = current.parent
    return None


def _extract_price(container, fallback_text: str) -> int | None:
    for tag in container.find_all(True):
        for key in ("data-price", "data-price-value", "data-priceuah", "data-price-uah"):
            value = tag.get(key)
            parsed = _parse_number(str(value)) if value else None
            if parsed is not None:
                return parsed

    for tag in container.select('[itemprop="price"]'):
        value = tag.get("content") or tag.get("data-price") or tag.get_text(" ", strip=True)
        parsed = _parse_number(value) if value else None
        if parsed is not None:
            return parsed

    price_candidates = []
    for tag in container.find_all(True, class_=True):
        classes = " ".join(tag.get("class") or []).lower()
        if "price" in classes and "old" not in classes and "credit" not in classes:
            price_candidates.extend(_parse_prices(tag.get_text(" ", strip=True)))
    if price_candidates:
        return price_candidates[0]

    all_prices = _parse_prices(fallback_text)
    if all_prices:
        return max(all_prices)

    loose_prices = _parse_large_numbers(fallback_text)
    if loose_prices:
        return max(loose_prices)
    return None


def _parse_prices(text: str) -> list[int]:
    text = _normalize_text(text)
    matches = _PRICE_RE.findall(text)
    values: list[int] = []
    for match in matches:
        raw = match[0] if isinstance(match, tuple) else match
        parsed = _parse_number(raw)
        if parsed is not None:
            values.append(parsed)
    return values


def _extract_price_from_text(text: str) -> int | None:
    prices = _parse_prices(text)
    if prices:
        return max(prices)
    large = _parse_large_numbers(text)
    if large:
        return max(large)
    return None


def _parse_large_numbers(text: str) -> list[int]:
    text = _normalize_text(text)
    matches = _NUMBER_RE.findall(text)
    values: list[int] = []
    for raw in matches:
        parsed = _parse_number(raw)
        if parsed is None:
            continue
        if parsed >= 1000:
            values.append(parsed)
    return values


def _parse_number(value: str) -> int | None:
    if not value:
        return None
    cleaned = re.sub(r"[^\d]", "", value)
    if not cleaned:
        return None
    return int(cleaned)


def _parse_shop(container) -> str | None:
    candidates: Iterable[str] = []
    links = container.find_all("a")
    texts = []
    for link in links:
        value = link.get_text(" ", strip=True)
        if value and value.upper() != "КУПИТИ":
            texts.append(value)
    candidates = texts

    for value in candidates:
        if 2 <= len(value) <= 40:
            return value
    return None


def _extract_original_url(container, base_url: str) -> str | None:
    for link in container.find_all("a"):
        href = link.get("href")
        if not href:
            continue
        normalized = _normalize_href(href, base_url)
        if "hotline.ua" not in normalized:
            return normalized
    return None


def _pick_shop_from_links(links: list[dict]) -> str | None:
    for link in links:
        raw = _normalize_text(link.get("text", ""))
        if not raw:
            continue
        text = raw.splitlines()[0].strip()
        if not text or text.lower() == "купити":
            continue
        if re.search(r"[0-9]", text):
            continue
        if 2 <= len(text) <= 40:
            return text
    return None


def _pick_offer_url_from_links(links: list[dict]) -> str | None:
    for link in links:
        href = link.get("href") or ""
        if "hotline.ua/go/" in href or "/go/" in href:
            return href
    for link in links:
        href = link.get("href") or ""
        if href:
            return href
    return None


def _pick_original_url_from_links(links: list[dict]) -> str | None:
    for link in links:
        href = link.get("href") or ""
        if not href:
            continue
        if href.startswith("/"):
            continue
        if href and "hotline.ua" not in href:
            return href
    return None


def _extract_title(container) -> str | None:
    for tag in container.find_all(True, class_=True):
        classes = " ".join(tag.get("class") or []).lower()
        if "title" in classes or "product" in classes:
            text = tag.get_text(" ", strip=True)
            if text and 5 <= len(text) <= 140:
                return text
    return None


def _extract_href(element, base_url: str) -> str | None:
    if element.name == "a" and element.get("href"):
        return _normalize_href(element.get("href"), base_url)
    parent = element.parent
    while parent is not None:
        if parent.name == "a" and parent.get("href"):
            return _normalize_href(parent.get("href"), base_url)
        parent = parent.parent
    return None


def _normalize_href(href: str, base_url: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"{base_url}{href}"
    return f"{base_url}/{href}"


def _detect_used(text: str) -> bool:
    lower = _normalize_text(text).lower()
    return "уцін" in lower or "віднов" in lower or "б/в" in lower or "уцен" in lower


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    return (
        text.replace("\u00a0", " ")
        .replace("\u202f", " ")
        .replace("\u2009", " ")
        .replace("\u2007", " ")
        .strip()
    )
