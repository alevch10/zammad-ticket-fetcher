import httpx
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from ..schemas import TicketSearchResponse, TicketArticlesResponse
from ..settings import settings
from ..app_logger import logger
import gc
import time
from datetime import datetime, timedelta


class ZammadClient:
    """
    Client for Zammad API. Adapted for prod format: direct list of tickets, no 'records'/total_count.
    Pagination by len(response) == limit (50); accumulate until <50.
    """

    def __init__(self):
        self.base_url = settings.zammad.url.rstrip("/")
        self.token = settings.zammad.token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        self.client = httpx.Client(
            timeout=60.0, verify=False
        )  # Prod SSL + longer timeout
        self.rps_delay = 1.0 / 3.0  # 3 RPS
        self.limit = 50  # Hardcoded for pagination check

    def _rate_limit(self):
        time.sleep(self.rps_delay)

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_fixed(1),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
        reraise=True,
    )
    def _make_request(
        self, method: str, endpoint: str, params: Dict[str, Any] = None
    ) -> Any:
        """
        Generic request; for tickets, returns List[Dict]; for articles, List[Dict].
        """
        url = f"{self.base_url}{endpoint}"
        logger.info(f"[{method}] {url} | params: {params}")

        try:
            if method.upper() == "GET":
                response = self.client.get(url, headers=self.headers, params=params)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            data = response.json()
            # Log size for lists
            if isinstance(data, list):
                logger.info(f"[{method}] {url} | SUCCESS | len(data): {len(data)}")
            else:
                logger.info(
                    f"[{method}] {url} | SUCCESS | type: {type(data)} | keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}"
                )
            self._rate_limit()
            return data
        except Exception as e:
            logger.error(
                f"[{method}] {url} | FAILED after retries | {type(e).__name__}: {str(e)} | response: {getattr(e, 'response', None).text if hasattr(e, 'response') else 'N/A'}"
            )
            raise

    def get_tickets_for_date(
        self, date_str: str, page: int = 1, use_range: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch one page as direct list (prod format).
        Returns raw list; parse with Pydantic outside if needed.
        Range query for full day.
        """
        query = f"created_at:{date_str}"

        params = {
            "query": query,
            "limit": self.limit,
            "page": page,
        }
        data = self._make_request("GET", "/api/v1/tickets/search", params)

        # Ensure it's list; debug if not
        if not isinstance(data, list):
            logger.warning(
                f"Unexpected response type for {date_str}: {type(data)} | FULL: {data}"
            )
            return []

        # Parse with schema for validation (optional; skips invalid records)
        try:
            parsed = TicketSearchResponse.model_validate(data)
            validated_tickets = [r.model_dump() for r in parsed.root]  # Back to dicts
            return validated_tickets
        except Exception as e:
            logger.error(
                f"Schema validation failed for {date_str}: {str(e)} | using raw data"
            )
            return data  # Fallback to raw list

    def fetch_all_tickets_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Paginate by checking len(page_data) == limit.
        Accumulate all_tickets until page returns < limit or empty.
        Filters applied on each page.
        """
        all_tickets = []
        page = 1
        use_range = True

        while True:
            logger.info(
                f"Fetching page {page} for {date_str} | total so far: {len(all_tickets)}"
            )
            page_data = self.get_tickets_for_date(date_str, page, use_range)

            if not page_data:
                # Fallback to simple query if first page empty
                if use_range and page == 1:
                    logger.info(f"Range empty; fallback to simple for {date_str}")
                    use_range = False
                    page = 1  # Reset page for fallback
                    continue
                else:
                    logger.warning(f"Empty page {page} for {date_str} | breaking")
                    break

            # Filter: skip ignored title; empty -> 'No Title'
            filtered_page = []
            skipped_count = 0
            for record in page_data:  # Raw dict now
                title = record.get("title", "").strip()
                if title == "Undelivered Mail Returned to Sender":
                    logger.warning(
                        f"Skipping ticket {record.get('id')} | ignored title"
                    )
                    skipped_count += 1
                    continue
                if not title:
                    title = "No Title"

                filtered = {
                    "id": record.get("id"),
                    "state": record.get("state_id"),
                    "title": title,
                    "article_count": record.get("article_count", 0),
                }
                # Validate min fields
                if filtered["id"] is None:
                    logger.warning(f"Invalid ticket {record} | missing id | skip")
                    skipped_count += 1
                    continue
                filtered_page.append(filtered)
                logger.debug(
                    f"Added ticket {filtered['id']} | title: {title[:50]}... | articles: {filtered['article_count']}"
                )

            all_tickets.extend(filtered_page)
            logger.info(
                f"Page {page} | added {len(filtered_page)} | skipped {skipped_count} | total now: {len(all_tickets)}"
            )

            # Pagination: continue if full page (assume more)
            if len(page_data) < self.limit:
                logger.info(
                    f"Partial page ({len(page_data)} < {self.limit}) | assuming end | total: {len(all_tickets)}"
                )
                break

            page += 1

        if not all_tickets:
            logger.warning(
                f"!!! NO TICKETS after pagination for {date_str} | check Zammad data/query !!!"
            )

        return all_tickets

    # get_articles_for_ticket unchanged (expects list)
    def get_articles_for_ticket(self, ticket_id: int) -> List[Dict[str, Any]]:
        endpoint = f"/api/v1/ticket_articles/by_ticket/{ticket_id}"
        try:
            data = self._make_request("GET", endpoint)  # Already list in prod?
            if isinstance(data, list):
                articles_resp = TicketArticlesResponse.model_validate(data)
            else:
                logger.warning(
                    f"Unexpected articles format for {ticket_id}: {type(data)} | FULL: {data[:200]}..."
                )  # Truncate log
                return []

            filtered_articles = []
            for article in articles_resp.root:
                if article.body:
                    filtered_articles.append(
                        {"from": article.from_field or "Unknown", "body": article.body}
                    )

            logger.info(
                f"Ticket {ticket_id} | SUCCESS | {len(filtered_articles)} articles"
            )
            return filtered_articles
        except Exception as e:
            logger.error(f"Ticket {ticket_id} | FAILED | {str(e)} | continuing")
            return []

    # process_day unchanged
    def process_day(self, date_str: str) -> List[Dict[str, Any]]:
        tickets = self.fetch_all_tickets_for_date(date_str)
        logger.info(f"Starting articles for {len(tickets)} tickets on {date_str}")

        enriched_tickets = []
        for idx, ticket in enumerate(tickets, 1):
            logger.info(
                f"[{idx}/{len(tickets)}] Articles for {ticket['id']} | expected: {ticket['article_count']}"
            )
            articles = self.get_articles_for_ticket(ticket["id"])

            enriched = ticket.copy()
            for i, art in enumerate(articles, 1):
                enriched[f"from_{i}"] = art["from"]
                enriched[f"body_{i}"] = art["body"]

            if ticket["article_count"] > 0 and not articles:
                logger.warning(
                    f"Ticket {ticket['id']} | expected {ticket['article_count']} but 0 articles"
                )

            enriched_tickets.append(enriched)

            if idx % 10 == 0:
                gc.collect()

        logger.info(f"Finished {date_str} | {len(enriched_tickets)} enriched")
        gc.collect()
        return enriched_tickets

    def close(self):
        self.client.close()
