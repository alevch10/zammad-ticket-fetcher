import httpx
from typing import List, Dict, Any
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    retry_if_exception_type,
)  # For 1 retry
from ..schemas import TicketSearchResponse, TicketArticlesResponse
from ..settings import settings
from ..app_logger import logger
import gc
import time  # For RPS limiter


class ZammadClient:
    """
    Client for Zammad API interactions.
    Handles auth, rate limiting (3 RPS), 1 retry on failures, pagination, and article fetching.
    Logs all requests/responses/errors for analysis.
    Sync-only for simplicity and to avoid overload.
    """

    def __init__(self):
        self.base_url = settings.zammad.url.rstrip("/")
        self.token = settings.zammad.token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        self.client = httpx.Client(timeout=30.0)  # 30s timeout; adjustable
        self.rps_delay = 1.0 / 3.0  # ~0.333s for 3 RPS; sleep after each request

    def _rate_limit(self):
        """Simple sleep for RPS control; called after each successful request."""
        time.sleep(self.rps_delay)
        # Comment: For production, consider token bucket (e.g., ratelimit lib), but this is lightweight

    @retry(
        stop=stop_after_attempt(2),  # 1 initial + 1 retry = 2 attempts
        wait=wait_fixed(1),  # Wait 1s before retry
        retry=retry_if_exception_type(
            (httpx.HTTPStatusError, httpx.TimeoutException)
        ),  # Retry only on HTTP/timeout errors
        reraise=True,  # Re-raise last exception if all retries fail
    )
    def _make_request(
        self, method: str, endpoint: str, params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Generic GET request with logging, 1 retry, and rate limiting.
        Logs full request/response; errors logged before raise for analysis.
        """
        url = f"{self.base_url}{endpoint}"
        logger.info(f"Making {method} request to {url} with params: {params}")

        try:
            if method.upper() == "GET":
                response = self.client.get(url, headers=self.headers, params=params)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            data = response.json()
            logger.info(
                f"Successful response from {url}: total keys={len(data)}"
            )  # Avoid logging full if huge; customize if needed
            self._rate_limit()  # Enforce RPS after success
            return data
        except Exception as e:
            logger.error(
                f"Request failed after retries for {url}: {type(e).__name__}: {str(e)}"
            )
            # Comment: No raise here in except; tenacity handles re-raise
            raise

    def get_tickets_for_date(
        self, date_str: str, page: int = 1
    ) -> TicketSearchResponse:
        """
        Fetch one page of tickets for a date.
        Parses with Pydantic for validation.
        """
        query = f"created_at:{date_str}"
        params = {
            "query": query,
            "expand": "false",
            "limit": 50,
            "page": page,
            "with_total_count": "true",
        }
        data = self._make_request("GET", "/api/v1/tickets/search", params)
        return TicketSearchResponse.model_validate(data)  # Pydantic parse

    def fetch_all_tickets_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Paginate to fetch ALL tickets for a date, combining records.
        Filters: ignore title "Undelivered Mail Returned to Sender".
        Logs progress and skips.
        """
        all_tickets = []
        page = 1
        while True:
            logger.info(f"Fetching page {page} for date {date_str}")
            response = self.get_tickets_for_date(date_str, page)

            if not response.records:
                break

            # Filter and enrich relevant fields
            filtered_tickets = []
            for record in response.records:
                if (
                    record.title == "Undelivered Mail Returned to Sender"
                ):  # Exact ignore as per update
                    logger.warning(
                        f"Skipping ticket {record.id} due to ignored title: {record.title}"
                    )
                    continue
                # Skip if no title or empty (as original, but now specific)
                if not record.title or not record.title.strip():
                    logger.warning(f"Skipping ticket {record.id} due to empty title")
                    continue
                filtered = {
                    "id": record.id,
                    "state": record.state_id,  # Map state_id to state
                    "title": record.title,
                    "article_count": record.article_count or 0,
                }
                filtered_tickets.append(filtered)

            all_tickets.extend(filtered_tickets)
            total_count = response.total_count or 0
            fetched = len(all_tickets)
            logger.info(
                f"Fetched {fetched}/{total_count} tickets for {date_str} (page {page})"
            )

            if fetched >= total_count:
                break

            page += 1

        return all_tickets

    def get_articles_for_ticket(self, ticket_id: int) -> List[Dict[str, Any]]:
        """
        Fetch ALL articles for a ticket (no pagination in Zammad for /by_ticket).
        Filters to {'from':, 'body':}; skips empty body.
        """
        endpoint = f"/api/v1/ticket_articles/by_ticket/{ticket_id}"
        data = self._make_request("GET", endpoint)
        articles_resp = TicketArticlesResponse.model_validate(data)  # Parse list

        filtered_articles = []
        for article in articles_resp.root:
            if article.body:  # Only if body present
                filtered_articles.append(
                    {
                        "from": article.from_field or "Unknown",  # Fallback
                        "body": article.body,
                    }
                )

        logger.info(f"Fetched {len(filtered_articles)} articles for ticket {ticket_id}")
        return filtered_articles

    def process_day(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Process one day: fetch tickets, articles for each, enrich with dynamic from/body.
        No limit on articles — all added as from_1/body_1 etc.
        Manual GC after day to prevent OOM on large ranges.
        """
        tickets = self.fetch_all_tickets_for_date(date_str)
        enriched_tickets = []

        for ticket in tickets:
            articles = self.get_articles_for_ticket(ticket["id"])
            # Enrich: copy base + dynamic keys for ALL articles (no limit)
            enriched = ticket.copy()
            for i, art in enumerate(articles, 1):
                enriched[f"from_{i}"] = art["from"]
                enriched[f"body_{i}"] = art["body"]
            enriched_tickets.append(enriched)

        logger.info(
            f"Processed {len(enriched_tickets)} tickets for {date_str} with max articles={max(t.get('article_count', 0) for t in enriched_tickets) if enriched_tickets else 0}"
        )
        gc.collect()  # Manual cleanup after day processing
        return enriched_tickets

    def close(self):
        """Close HTTP client on app shutdown to free connections."""
        self.client.close()
        # Comment: Call in @app.on_event("shutdown")
