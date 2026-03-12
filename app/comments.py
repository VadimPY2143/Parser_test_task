import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from app.models import BrainComment, CommentItem, ComfyComment

logger = logging.getLogger(__name__)


def normalize_product_url(raw_url: str) -> str:
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


def detect_source(url: str) -> str:
    host = urlsplit(url).netloc.lower()
    if "comfy.ua" in host:
        return "comfy"
    if "brain.com.ua" in host:
        return "brain"
    return "unknown"


async def get_comments(url: str, date_to: datetime | None) -> tuple[str, list[CommentItem]]:
    clean_url = normalize_product_url(url)
    source = detect_source(url)
    if source == "comfy":
        comments = await _fetch_comfy_comments(url)
    elif source == "brain":
        comments = await _fetch_brain_comments(url)
    else:
        return clean_url, []

    filtered = _filter_by_date(comments, date_to)
    return clean_url, filtered


async def _fetch_comfy_comments(url: str) -> list[CommentItem]:
    async with async_playwright() as playwright:
        headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
        browser = await playwright.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            locale="uk-UA",
            timezone_id="Europe/Kiev",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/26.3 Safari/605.1.15"
            ),
            viewport={"width": 1440, "height": 900},
        )
        await _apply_stealth(context)
        page = await context.new_page()
        captured: list[dict] = []

        try:
            _attach_response_listener(page, captured)
            await page.goto(url, wait_until="domcontentloaded")
            await _open_reviews_tab(page)
            await _wait_for_reviews(page, max_wait_ms=20000)
            await _expand_reviews(page)
            await _scroll_and_wait(page)
            await _expand_reviews(page)
            html = await page.content()
        finally:
            await context.close()
            await browser.close()

    json_comments = _extract_comments_from_json(captured)
    if json_comments:
        return json_comments
    comments = _parse_comments_from_html(html, source="comfy")
    _debug_dump("comfy", url, html, captured, comments)
    return comments


async def _fetch_brain_comments(url: str) -> list[CommentItem]:
    async with async_playwright() as playwright:
        headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
        browser = await playwright.chromium.launch(
            headless=headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            locale="uk-UA",
            timezone_id="Europe/Kiev",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/26.3 Safari/605.1.15"
            ),
            viewport={"width": 1440, "height": 900},
        )
        await _apply_stealth(context)
        page = await context.new_page()
        captured: list[dict] = []

        try:
            _attach_response_listener(page, captured)
            await page.goto(url, wait_until="domcontentloaded")
            await _open_reviews_tab(page)
            await _wait_for_reviews(page, max_wait_ms=20000)
            await _expand_reviews(page)
            await _scroll_and_wait(page)
            await _expand_reviews(page)
            html = await page.content()
        finally:
            await context.close()
            await browser.close()

    json_comments = _extract_comments_from_json(captured)
    if json_comments:
        return json_comments
    comments = _parse_comments_from_html(html, source="brain")
    _debug_dump("brain", url, html, captured, comments)
    return comments


async def _open_reviews_tab(page) -> None:
    for text in ("Відгуки", "Отзывы", "Reviews"):
        locator = page.get_by_text(text, exact=False)
        if await locator.count() > 0:
            await locator.first.scroll_into_view_if_needed()
            await locator.first.click(force=True)
            return

    await page.evaluate(
        """
        () => {
          const el = [...document.querySelectorAll('*')].find(e => {
            const t = e.textContent?.trim().toLowerCase();
            return t === 'відгуки' || t === 'отзывы' || t === 'reviews';
          });
          if (el) el.click();
        }
        """
    )


async def _wait_for_reviews(page, max_wait_ms: int) -> None:
    selector = (
        "[class*='review'], [class*='reviews'], [class*='comment'], "
        "[class*='feedback'], [itemprop='review'], [data-review-id], [data-comment-id]"
    )
    step_ms = 500
    waited = 0
    while waited < max_wait_ms:
        if await page.locator(selector).count() > 0:
            return
        await page.wait_for_timeout(step_ms)
        waited += step_ms


async def _apply_stealth(context) -> None:
    await context.add_init_script(
        """
        () => {
          Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
          Object.defineProperty(navigator, 'languages', {get: () => ['uk-UA', 'uk', 'en']});
          Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        }
        """
    )


async def _expand_reviews(page) -> None:
    for _ in range(3):
        for text in ("Показати ще", "Показать еще", "Ще", "Load more"):
            locator = page.get_by_text(text, exact=False)
            if await locator.count() > 0:
                try:
                    await locator.first.scroll_into_view_if_needed()
                    await locator.first.click(force=True)
                    await page.wait_for_timeout(800)
                except Exception:
                    pass


async def _scroll_and_wait(page) -> None:
    for _ in range(3):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            return
        await page.wait_for_timeout(1000)


def _attach_response_listener(page, captured: list[dict]) -> None:
    async def handle_response(response) -> None:
        try:
            if response.request.resource_type not in {"xhr", "fetch"}:
                return
            text = await response.text()
            if not text or len(text) > 1_000_000:
                return
            stripped = text.lstrip()
            if not stripped or stripped[0] not in "{[":
                return
            data = json.loads(text)
            captured.append(data)
        except Exception:
            return

    page.on("response", lambda resp: asyncio.create_task(handle_response(resp)))


def _parse_comments_from_html(html: str, source: str) -> list[CommentItem]:
    inline_reviews = _extract_inline_reviews_from_html(html)
    if source == "brain":
        if _brain_has_no_reviews(html):
            return []
        brain_reviews = _extract_brain_reviews_from_text(html)
        if inline_reviews and brain_reviews:
            return _merge_comments(inline_reviews, brain_reviews)
        if inline_reviews:
            return inline_reviews
        jsonld_comments = _extract_jsonld_comments(html)
        if jsonld_comments and brain_reviews:
            return _merge_comments(jsonld_comments, brain_reviews)
        if jsonld_comments:
            return jsonld_comments
        if brain_reviews:
            return brain_reviews
    if inline_reviews:
        return inline_reviews
    jsonld_comments = _extract_jsonld_comments(html)
    if jsonld_comments:
        return jsonld_comments

    soup = BeautifulSoup(html, "html.parser")
    blocks = _find_comment_blocks(soup)

    comments: list[CommentItem] = []
    for block in blocks:
        rating = _extract_rating(block)
        advantages = _extract_section_text(block, ["Переваги", "Достоинства"])
        shortcomings = _extract_section_text(block, ["Недоліки", "Недостатки"])
        comment = _extract_comment_text(block)
        created_at = _extract_date(block)

        if source == "comfy":
            item = ComfyComment(
                rating=rating,
                advantages=advantages,
                shortcomings=shortcomings,
                comment=comment,
                created_at=created_at,
            )
        else:
            item = BrainComment(
                rating=rating,
                advantages=advantages,
                shortcomings=shortcomings,
                comment=comment,
                created_at=created_at,
            )

        comments.append(
            CommentItem(
                rating=item.rating,
                advantages=item.advantages,
                shortcomings=item.shortcomings,
                comment=item.comment,
                created_at=item.created_at,
            )
        )

    return comments


def _extract_inline_reviews_from_html(html: str) -> list[CommentItem]:
    for key in ("topReviews", "reviews", "review", "feedbacks", "comments", "opinions", "testimonials"):
        reviews = _extract_json_array_after_key(html, key)
        if reviews:
            filtered = [item for item in reviews if _looks_like_review_dict(item)]
            if filtered:
                return _map_review_list(filtered)
    return []


def _extract_brain_reviews_from_text(html: str) -> list[CommentItem]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    if not text:
        return []
    text = _slice_brain_reviews_text(text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    results: list[CommentItem] = []
    seen: set[str] = set()

    idx = 0
    while idx < len(lines):
        line = lines[idx]
        date = _parse_date(line)
        if date is None:
            idx += 1
            continue
        author = lines[idx - 1] if idx > 0 else ""
        if _brain_is_ui_line(author):
            idx += 1
            continue
        if "brain" in author.lower():
            idx += 1
            continue
        comment_lines: list[str] = []
        j = idx + 1
        while j < len(lines):
            if _parse_date(lines[j]) is not None:
                break
            if _brain_is_ui_line(lines[j]):
                j += 1
                continue
            comment_lines.append(lines[j])
            j += 1
        comment = _clean_text(" ".join(comment_lines))
        if comment:
            comment = re.sub(r"\bпрофесіонал brain\b", "", comment, flags=re.IGNORECASE).strip()
        if comment and comment.lower() not in seen:
            seen.add(comment.lower())
            results.append(
                CommentItem(
                    rating=None,
                    advantages="",
                    shortcomings="",
                    comment=comment,
                    created_at=date,
                )
            )
        idx = j

    return results


def _slice_brain_reviews_text(text: str) -> str:
    lower = text.lower()
    start = lower.find("найкорисніші відгуки")
    if start == -1:
        start = lower.find("відгуки")
    if start != -1:
        text = text[start:]
        lower = text.lower()
    for marker in ("аксесуари", "характеристики", "опис", "фото"):
        end = lower.find(marker)
        if end != -1:
            return text[:end]
    return text


def _brain_is_ui_line(line: str) -> bool:
    lower = line.lower()
    if not lower:
        return True
    ui_phrases = (
        "залишити відгук",
        "ваша оцінка",
        "ваше ім'я",
        "ваш e-mail",
        "ваш коментар",
        "максимальна кількість символів",
        "поділитися",
        "відповісти",
        "приховати",
        "надiслати відповідь",
        "надіслати відповідь",
        "оцінка",
        "progress",
        "like",
        "dislike",
        "професіонал brain",
    )
    if any(phrase in lower for phrase in ui_phrases):
        return True
    if re.fullmatch(r"[\d\s\(\)\.]+", lower):
        return True
    if lower.startswith("image"):
        return True
    return False


def _brain_has_no_reviews(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True).lower()
    markers = (
        "відгуків ще немає",
        "відгуків немає",
        "немає відгуків",
        "be the first to review",
        "be the first",
        "0 відгуків"
    )
    return any(marker in text for marker in markers)


def _extract_json_array_after_key(html: str, key: str) -> list[dict]:
    marker = f'"{key}":'
    idx = html.find(marker)
    if idx == -1:
        return []
    start = html.find("[", idx)
    if start == -1:
        return []
    end = _find_matching_bracket(html, start)
    if end == -1:
        return []
    raw = html[start : end + 1]
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return _parse_loose_review_array(raw)


def _parse_loose_review_array(raw: str) -> list[dict]:
    items: list[dict] = []
    i = 0
    length = len(raw)
    while i < length:
        start = raw.find("{", i)
        if start == -1:
            break
        end = _find_matching_brace(raw, start)
        if end == -1:
            break
        chunk = raw[start : end + 1]
        try:
            item = json.loads(chunk)
            if isinstance(item, dict):
                items.append(item)
        except Exception:
            pass
        i = end + 1
    return items


def _find_matching_brace(text: str, start: int) -> int:
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _debug_dump(source: str, url: str, html: str, captured: list[dict], comments: list[CommentItem]) -> None:
    if os.getenv("COMMENTS_DEBUG") != "1":
        return
    try:
        debug_dir = Path("/tmp/comments-debug")
        debug_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_source = source.replace("/", "_")
        (debug_dir / f"{safe_source}_{stamp}.html").write_text(html, encoding="utf-8")

        meta = {
            "url": url,
            "source": source,
            "captured_json_count": len(captured),
            "comments_count": len(comments),
            "topReviews_found": "\"topReviews\"" in html,
            "reviews_found": "\"reviews\"" in html,
            "html_size": len(html),
        }
        (debug_dir / f"{safe_source}_{stamp}.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        candidates = [
            payload
            for payload in captured
            if _payload_has_keywords(
                payload,
                {
                    "review",
                    "reviews",
                    "topReviews",
                    "comment",
                    "rating",
                    "feedback",
                    "opinions",
                    "testimonials",
                    "reviewId",
                },
            )
        ]
        for idx, payload in enumerate(candidates[:3], start=1):
            (debug_dir / f"{safe_source}_{stamp}_payload_{idx}.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        logger.info("Comments debug written to %s", debug_dir)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to write debug dump: %s", exc)


def _payload_has_keywords(payload, keywords: set[str]) -> bool:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str) and key in keywords:
                return True
            if _payload_has_keywords(value, keywords):
                return True
    elif isinstance(payload, list):
        for item in payload:
            if _payload_has_keywords(item, keywords):
                return True
    return False


def _find_matching_bracket(text: str, start: int) -> int:
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _map_review_list(reviews: list[dict]) -> list[CommentItem]:
    results: list[CommentItem] = []
    for review in reviews:
        if not isinstance(review, dict):
            continue
        comment, rating, advantages, shortcomings, created_at = _extract_review_fields(review)
        if not comment:
            continue
        results.append(
            CommentItem(
                rating=rating,
                advantages=advantages,
                shortcomings=shortcomings,
                comment=comment,
                created_at=created_at,
            )
        )
    return results


def _extract_jsonld_comments(html: str) -> list[CommentItem]:
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    comments: list[CommentItem] = []
    for script in scripts:
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        for item in _iter_jsonld_items(data):
            reviews = item.get("review") or []
            if isinstance(reviews, dict):
                reviews = [reviews]
            for review in reviews:
                if not isinstance(review, dict):
                    continue
                rating = _pick_rating(review)
                comment = _clean_text(str(review.get("reviewBody") or ""))
                created_at = _pick_date(review)
                if not comment:
                    continue
                comments.append(
                    CommentItem(
                        rating=rating,
                        advantages="",
                        shortcomings="",
                        comment=comment,
                        created_at=created_at,
                    )
                )
    return comments


def _extract_comments_from_json(payloads: list[dict]) -> list[CommentItem]:
    comments: list[CommentItem] = []
    seen: set[tuple[str, str]] = set()

    for payload in payloads:
        top_reviews = _extract_top_reviews(payload)
        if top_reviews:
            for item in top_reviews:
                key = (item.comment, str(item.created_at))
                if key in seen:
                    continue
                seen.add(key)
                comments.append(item)
            continue

        reviews_list = _extract_reviews_list(payload)
        if reviews_list:
            for item in reviews_list:
                key = (item.comment, str(item.created_at))
                if key in seen:
                    continue
                seen.add(key)
                comments.append(item)
            continue

        direct_reviews = _extract_review_objects(payload)
        if direct_reviews:
            for item in direct_reviews:
                key = (item.comment, str(item.created_at))
                if key in seen:
                    continue
                seen.add(key)
                comments.append(item)

        generic_reviews = _extract_generic_review_lists(payload)
        if generic_reviews:
            for item in generic_reviews:
                key = (item.comment, str(item.created_at))
                if key in seen:
                    continue
                seen.add(key)
                comments.append(item)

    return comments


def _extract_top_reviews(payload: dict) -> list[CommentItem]:
    results: list[CommentItem] = []
    for item in _iter_dicts(payload):
        reviews = item.get("topReviews")
        if not isinstance(reviews, list):
            continue
        for review in reviews:
            if not isinstance(review, dict):
                continue
            comment, rating, advantages, shortcomings, created_at = _extract_review_fields(review)
            if not comment:
                continue
            results.append(
                CommentItem(
                    rating=rating,
                    advantages=advantages,
                    shortcomings=shortcomings,
                    comment=comment,
                    created_at=created_at,
                )
            )
    return results


def _extract_reviews_list(payload: dict) -> list[CommentItem]:
    results: list[CommentItem] = []
    for item in _iter_dicts(payload):
        reviews = item.get("reviews")
        if not isinstance(reviews, list):
            continue
        for review in reviews:
            if not isinstance(review, dict):
                continue
            comment, rating, advantages, shortcomings, created_at = _extract_review_fields(review)
            if not comment:
                continue
            results.append(
                CommentItem(
                    rating=rating,
                    advantages=advantages,
                    shortcomings=shortcomings,
                    comment=comment,
                    created_at=created_at,
                )
            )
    return results


def _extract_review_objects(payload: dict) -> list[CommentItem]:
    results: list[CommentItem] = []
    for item in _iter_dicts(payload):
        if not isinstance(item, dict):
            continue
        if "reviewId" not in item or "detail" not in item:
            continue
        comment, rating, advantages, shortcomings, created_at = _extract_review_fields(item)
        if not comment:
            continue
        results.append(
            CommentItem(
                rating=rating,
                advantages=advantages,
                shortcomings=shortcomings,
                comment=comment,
                created_at=created_at,
            )
        )
    return results


def _extract_generic_review_lists(payload: dict) -> list[CommentItem]:
    results: list[CommentItem] = []
    seen_lists: set[int] = set()
    for item in _iter_dicts(payload):
        if not isinstance(item, dict):
            continue
        for value in item.values():
            if not isinstance(value, list):
                continue
            if id(value) in seen_lists:
                continue
            seen_lists.add(id(value))
            dict_items = [entry for entry in value if isinstance(entry, dict)]
            if not dict_items:
                continue
            if not any(_looks_like_review_dict(entry) for entry in dict_items):
                continue
            for review in dict_items:
                if not _looks_like_review_dict(review):
                    continue
                comment, rating, advantages, shortcomings, created_at = _extract_review_fields(review)
                if not comment:
                    continue
                results.append(
                    CommentItem(
                        rating=rating,
                        advantages=advantages,
                        shortcomings=shortcomings,
                        comment=comment,
                        created_at=created_at,
                    )
                )
    return results


def _iter_dicts(value):
    if isinstance(value, dict):
        yield value
        for v in value.values():
            yield from _iter_dicts(v)
    elif isinstance(value, list):
        for item in value:
            yield from _iter_dicts(item)


def _pick_text_field(item: dict, keys: list[str]) -> str | None:
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _looks_like_review_dict(item: dict) -> bool:
    if not isinstance(item, dict):
        return False
    text = _pick_text_field(
        item,
        ["detail", "comment", "review", "text", "reviewBody"],
    )
    has_text = bool(text)
    has_rating = _pick_rating(item) is not None
    has_date = _pick_date(item) is not None
    has_id = "reviewId" in item
    has_meta = any(
        key in item
        for key in (
            "advantages",
            "disadvantages",
            "pros",
            "cons",
            "pluses",
            "minuses",
            "shortcomings",
        )
    )
    return has_text and (has_rating or has_date or has_id or has_meta)


def _pick_rating(item: dict) -> float | None:
    for key in ("rating", "rate", "score", "stars", "value"):
        value = item.get(key)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            parsed = _extract_number(value)
            if parsed is not None:
                return float(parsed)
    if isinstance(item.get("reviewRating"), dict):
        value = item.get("reviewRating", {}).get("ratingValue")
        if value is not None:
            parsed = _extract_number(str(value))
            if parsed is not None:
                return float(parsed)
    if "productRating" in item:
        return _normalize_rating(item.get("productRating"))
    return None


def _normalize_rating(value) -> float | None:
    if value is None:
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        parsed = _extract_number(str(value))
        if parsed is None:
            return None
        num = float(parsed)
    if num > 10:
        return round(num / 20, 2)
    return num


def _pick_date(item: dict) -> datetime | None:
    for key in (
        "created_at",
        "createdAt",
        "created",
        "date",
        "datePublished",
        "published",
        "formattedCreatedAt",
    ):
        value = item.get(key)
        if isinstance(value, str):
            parsed = _parse_date(value)
            if parsed is not None:
                return parsed
    return None


def _extract_review_fields(
    review: dict,
) -> tuple[str, float | None, str, str, datetime | None]:
    comment = _pick_text_field(
        review,
        ["detail", "comment", "review", "text", "reviewBody"],
    )
    rating = _pick_rating(review)
    advantages = _pick_text_field(review, ["advantages", "pros", "pluses"]) or ""
    shortcomings = _pick_text_field(review, ["disadvantages", "cons", "minuses", "shortcomings"]) or ""
    created_at = _pick_date(review)

    return (
        _clean_text(comment or ""),
        rating,
        _clean_text(advantages),
        _clean_text(shortcomings),
        created_at,
    )


def _iter_jsonld_items(data):
    if isinstance(data, dict):
        if "@graph" in data and isinstance(data["@graph"], list):
            for item in data["@graph"]:
                if isinstance(item, dict):
                    yield item
        else:
            yield data
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item


def _find_comment_blocks(soup: BeautifulSoup) -> list:
    selectors = [
        "[class*='review']",
        "[class*='reviews']",
        "[class*='comment']",
        "[class*='feedback']",
        "[itemprop='review']",
        "[data-review-id]",
        "[data-comment-id]",
    ]
    blocks = []
    for selector in selectors:
        blocks.extend(soup.select(selector))
    return _dedupe_blocks(blocks)


def _dedupe_blocks(blocks: list) -> list:
    seen = set()
    result = []
    for block in blocks:
        key = id(block)
        if key in seen:
            continue
        seen.add(key)
        text = block.get_text(" ", strip=True)
        if len(text) < 20:
            continue
        result.append(block)
    return result


def _extract_rating(block) -> float | None:
    for attr in ("data-rating", "data-rate", "data-score"):
        value = block.get(attr)
        if value:
            return _to_float(value)
    for tag in block.select("[class*='rating'], [class*='stars']"):
        value = tag.get("data-rating") or tag.get("data-rate") or tag.get_text(" ", strip=True)
        rating = _extract_number(value)
        if rating is not None:
            return rating
    for tag in block.select("[aria-label], [title], [data-value]"):
        value = tag.get("aria-label") or tag.get("title") or tag.get("data-value")
        rating = _extract_number(value)
        if rating is not None and 0 < rating <= 10:
            if rating > 5:
                rating = rating / 2
            return rating
    return None


def _extract_section_text(block, labels: list[str]) -> str:
    for label in labels:
        el = block.find(string=re.compile(label, re.IGNORECASE))
        if el is None:
            continue
        parent = el.parent
        if parent is None:
            continue
        text = parent.get_text(" ", strip=True)
        cleaned = text.replace(label, "").strip(": ")
        if cleaned:
            return cleaned
    return ""


def _extract_comment_text(block) -> str:
    for selector in ("[class*='text']", "[class*='body']"):
        el = block.select_one(selector)
        if el:
            text = el.get_text(" ", strip=True)
            if text:
                return text
    return block.get_text(" ", strip=True)


def _extract_date(block) -> datetime | None:
    time_el = block.find("time")
    if time_el and time_el.get("datetime"):
        return _parse_date(time_el.get("datetime"))
    text = block.get_text(" ", strip=True)
    return _parse_date(text)


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip()
    patterns = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%d.%m.%Y",
    ]
    for pattern in patterns:
        try:
            return datetime.strptime(cleaned, pattern)
        except ValueError:
            continue
    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", cleaned)
    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            return None
    month_match = re.search(
        r"(\d{1,2})\s+([A-Za-zА-Яа-яіїєґІЇЄҐ]+)\s+(\d{4})",
        cleaned,
    )
    if month_match:
        day, month_name, year = month_match.groups()
        month = _month_name_to_number(month_name)
        if month is not None:
            try:
                return datetime(int(year), month, int(day))
            except ValueError:
                return None
    return None


def _month_name_to_number(value: str) -> int | None:
    normalized = value.strip().lower()
    mapping = {
        "січня": 1,
        "лютого": 2,
        "березня": 3,
        "квітня": 4,
        "травня": 5,
        "червня": 6,
        "липня": 7,
        "серпня": 8,
        "вересня": 9,
        "жовтня": 10,
        "листопада": 11,
        "грудня": 12,
        "января": 1,
        "февраля": 2,
        "марта": 3,
        "апреля": 4,
        "мая": 5,
        "июня": 6,
        "июля": 7,
        "августа": 8,
        "сентября": 9,
        "октября": 10,
        "ноября": 11,
        "декабря": 12,
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    return mapping.get(normalized)


def _clean_text(value: str) -> str:
    if not value:
        return ""
    text = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _comment_key(comment: str) -> str:
    cleaned = _clean_text(comment).lower()
    cleaned = re.sub(r"[^\w\s]", "", cleaned, flags=re.UNICODE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def _merge_comments(primary: list[CommentItem], secondary: list[CommentItem]) -> list[CommentItem]:
    secondary_map: dict[str, CommentItem] = {}
    for item in secondary:
        key = _comment_key(item.comment)
        if key:
            secondary_map[key] = item
    used: set[str] = set()
    results: list[CommentItem] = []
    for item in primary:
        key = _comment_key(item.comment)
        other = secondary_map.get(key)
        if other:
            used.add(key)
            results.append(
                CommentItem(
                    rating=item.rating if item.rating is not None else other.rating,
                    advantages=item.advantages or other.advantages,
                    shortcomings=item.shortcomings or other.shortcomings,
                    comment=item.comment,
                    created_at=item.created_at or other.created_at,
                )
            )
        else:
            results.append(item)
    for key, item in secondary_map.items():
        if key in used:
            continue
        results.append(item)
    return results


def _filter_by_date(comments: list[CommentItem], date_to: datetime | None) -> list[CommentItem]:
    if date_to is None:
        return comments
    result: list[CommentItem] = []
    for item in comments:
        if item.created_at is None:
            continue
        if item.created_at <= date_to:
            result.append(item)
    return result


def _extract_number(value: str | None) -> float | None:
    if not value:
        return None
    match = re.search(r"(\d+(?:[\.,]\d+)?)", value)
    if not match:
        return None
    return _to_float(match.group(1))


def _to_float(value: str) -> float | None:
    try:
        return float(value.replace(",", "."))
    except ValueError:
        return None
