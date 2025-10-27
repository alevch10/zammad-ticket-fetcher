import httpx
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from ..schemas import TicketSearchResponse, TicketArticlesResponse
from ..settings import settings
from ..app_logger import logger
import gc
import time


class ZammadClient:
    """
    Client for prod Zammad format: {'tickets': [IDs], 'tickets_count': total, 'assets': {'Ticket': {ID: full_ticket}}}
    Simplified: Always use exact 'created_at:{date_str}' query (no range for simplicity).
    Pagination based on tickets_count and partial pages.
    """

    def __init__(self):
        self.base_url = settings.zammad.url.rstrip("/")
        self.token = settings.zammad.token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        self.client = httpx.Client(timeout=60.0, verify=False)
        self.rps_delay = 1.0 / 3.0
        self.limit = 50  # For pagination check

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
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        logger.info(f"[{method}] {url} | params: {params}")

        try:
            response = self.client.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            # Log structure
            tickets_count = data.get("tickets_count", "N/A")
            assets_len = (
                len(data.get("assets", {}).get("Ticket", {}))
                if data.get("assets")
                else 0
            )
            logger.info(
                f"[{method}] {url} | SUCCESS | tickets_count: {tickets_count} | assets.Ticket len: {assets_len}"
            )
            self._rate_limit()
            return data
        except Exception as e:
            logger.error(f"[{method}] {url} | FAILED | {type(e).__name__}: {str(e)}")
            raise

    def get_tickets_for_date(self, date_str: str, page: int = 1) -> Dict[str, Any]:
        """
        Fetch raw response dict; always exact day query for simplicity.
        Caller extracts assets.Ticket.
        """
        query = f"created_at:{date_str}"  # Exact day; simplified, no range
        logger.info(f"Using exact day query for {date_str}: {query} (page {page})")

        params = {
            "query": query,
            "expand": "false",
            "limit": self.limit,
            "page": page,
            "with_total_count": "true",
        }
        data = self._make_request("GET", "/api/v1/tickets/search", params)

        # Validate with schema (optional)
        try:
            validated = TicketSearchResponse.model_validate(data)
            logger.debug(f"Schema validated for {date_str} page {page}")
        except Exception as e:
            logger.warning(
                f"Schema validation failed for {date_str}: {str(e)} | using raw"
            )

        # Debug empty
        if not data.get("assets", {}).get("Ticket"):
            logger.warning(
                f"Empty assets.Ticket for {date_str} page {page} | FULL: {data}"
            )

        return data

    def fetch_all_tickets_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Paginate using exact day query.
        Extract full tickets from assets.Ticket.values().
        Stop when fetched >= tickets_count or partial page.
        """
        all_tickets = []
        page = 1
        total_count = 0
        fetched = 0

        while True:
            logger.info(
                f"Fetching page {page} for {date_str} | total so far: {len(all_tickets)}"
            )
            response = self.get_tickets_for_date(date_str, page)

            # Extract page tickets from assets
            page_tickets_raw = response.get("assets", {}).get("Ticket", {})
            page_tickets = list(page_tickets_raw.values())  # List[full ticket dicts]

            if not page_tickets:
                logger.warning(f"Empty page {page} for {date_str} | breaking")
                break

            # Update total if not set
            if total_count == 0:
                total_count = response.get("tickets_count", len(page_tickets))
                logger.info(f"Set total_count: {total_count} for {date_str}")

            # Filter page_tickets
            filtered_page = []
            skipped_count = 0
            for ticket in page_tickets:
                title = ticket.get("title", "").strip()
                if title == "Undelivered Mail Returned to Sender":
                    logger.warning(
                        f"Skipping ticket {ticket.get('id')} | ignored title"
                    )
                    skipped_count += 1
                    continue
                if not title:
                    title = "No Title"

                filtered = {
                    "id": ticket.get("id"),
                    "state": ticket.get("state_id"),
                    "title": title,
                    "article_count": ticket.get("article_count", 0),
                }
                if filtered["id"] is None:
                    skipped_count += 1
                    continue
                filtered_page.append(filtered)
                logger.debug(
                    f"Added ticket {filtered['id']} | title: {title[:50]}... | articles: {filtered['article_count']}"
                )

            all_tickets.extend(filtered_page)
            fetched += len(filtered_page)
            logger.info(
                f"Page {page} | extracted {len(page_tickets)} | added {len(filtered_page)} | skipped {skipped_count} | fetched/total: {fetched}/{total_count}"
            )

            # Pagination: Stop if partial page or reached total
            if len(page_tickets) < self.limit or fetched >= total_count:
                logger.info(
                    f"End of pagination for {date_str} | final total: {len(all_tickets)}"
                )
                break

            page += 1

        if not all_tickets:
            logger.warning(
                f"!!! NO TICKETS for {date_str} | check query/data in Zammad (exact day may miss time-based) !!!"
            )

        return all_tickets

    # get_articles_for_ticket unchanged
    def get_articles_for_ticket(self, ticket_id: int) -> List[Dict[str, Any]]:
        endpoint = f"/api/v1/ticket_articles/by_ticket/{ticket_id}"
        try:
            data = self._make_request("GET", endpoint)
            articles_resp = TicketArticlesResponse.model_validate(data)
            filtered_articles = [
                {"from": art.from_field or "Unknown", "body": art.body}
                for art in articles_resp.root
                if art.body
            ]
            logger.info(f"Ticket {ticket_id} | {len(filtered_articles)} articles")
            return filtered_articles
        except Exception as e:
            logger.error(f"Ticket {ticket_id} | FAILED | {str(e)}")
            return []

    # process_day unchanged
    def process_day(self, date_str: str) -> List[Dict[str, Any]]:
        tickets = self.fetch_all_tickets_for_date(date_str)
        logger.info(f"Articles for {len(tickets)} tickets on {date_str}")
        enriched_tickets = []
        for idx, ticket in enumerate(tickets, 1):
            articles = self.get_articles_for_ticket(ticket["id"])
            enriched = ticket.copy()
            for i, art in enumerate(articles, 1):
                enriched[f"from_{i}"] = art["from"]
                enriched[f"body_{i}"] = art["body"]
            if ticket["article_count"] > 0 and not articles:
                logger.warning(
                    f"Ticket {ticket['id']} | expected {ticket['article_count']} but 0"
                )
            enriched_tickets.append(enriched)
            if idx % 10 == 0:
                gc.collect()
        logger.info(f"Finished {date_str} | {len(enriched_tickets)} enriched")
        gc.collect()
        return enriched_tickets

    def close(self):
        self.client.close()
