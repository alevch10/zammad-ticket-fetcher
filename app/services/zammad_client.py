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
    Client for Zammad API. Updated with extra logging for prod debug.
    Handles empty tickets, errors in articles without full stop.
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
        )  # Increased timeout for large bodies; verify=False for prod SSL
        self.rps_delay = 1.0 / 3.0  # 3 RPS

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
        logger.info(
            f"[{method}] {url} | params: {params}"
        )  # Extra log for endpoint tracking

        try:
            if method.upper() == "GET":
                response = self.client.get(url, headers=self.headers, params=params)
            else:
                raise ValueError(f"Unsupported method: {method}")

            response.raise_for_status()
            data = response.json()
            logger.info(
                f"[{method}] {url} | SUCCESS | records/total: {len(data.get('records', [])) if 'records' in data else 'N/A'}/{data.get('total_count', 'N/A')}"
            )  # Specific for tickets/articles
            self._rate_limit()
            return data
        except Exception as e:
            logger.error(
                f"[{method}] {url} | FAILED after retries | {type(e).__name__}: {str(e)} | response: {getattr(e, 'response', {}).text if hasattr(e, 'response') else 'N/A'}"
            )
            raise

    def get_tickets_for_date(
        self, date_str: str, page: int = 1
    ) -> TicketSearchResponse:
        query = f"created_at:{date_str}"
        params = {
            "query": query,
            "expand": "false",
            "limit": 50,
            "page": page,
            "with_total_count": "true",
        }
        data = self._make_request("GET", "/api/v1/tickets/search", params)
        return TicketSearchResponse.model_validate(data)

    def fetch_all_tickets_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        all_tickets = []
        page = 1
        while True:
            logger.info(
                f"Fetching page {page} for {date_str} | total so far: {len(all_tickets)}"
            )
            response = self.get_tickets_for_date(date_str, page)

            if not response.records:
                logger.warning(f"No records on page {page} for {date_str} | breaking")
                break

            filtered_tickets = []
            skipped_count = 0
            for record in response.records:
                # Updated filter: Skip ONLY specific phrase; empty title -> 'No Title' to not lose tickets
                if record.title == "Undelivered Mail Returned to Sender":
                    logger.warning(
                        f"Skipping ticket {record.id} | ignored title: {record.title}"
                    )
                    skipped_count += 1
                    continue
                title = (
                    record.title.strip() if record.title else "No Title"
                )  # Fallback, not skip
                if not title:  # Extra safe
                    title = "Empty Title"

                filtered = {
                    "id": record.id,
                    "state": record.state_id,
                    "title": title,
                    "article_count": record.article_count or 0,
                }
                filtered_tickets.append(filtered)
                logger.debug(
                    f"Added ticket {record.id} | title: {title[:50]}... | articles: {filtered['article_count']}"
                )

            all_tickets.extend(filtered_tickets)
            total_count = response.total_count or 0
            logger.info(
                f"Page {page} | added {len(filtered_tickets)} | skipped {skipped_count} | total: {len(all_tickets)}/{total_count}"
            )

            if len(all_tickets) >= total_count:
                break

            page += 1

        if not all_tickets:
            logger.warning(
                f"!!! NO TICKETS after all pages for {date_str} | check date/query/token !!!"
            )

        return all_tickets

    def get_articles_for_ticket(self, ticket_id: int) -> List[Dict[str, Any]]:
        """
        Fetch articles with try-except: log error, return [] to continue processing other tickets.
        """
        endpoint = f"/api/v1/ticket_articles/by_ticket/{ticket_id}"
        try:
            data = self._make_request("GET", endpoint)
            articles_resp = TicketArticlesResponse.model_validate(data)

            filtered_articles = []
            for article in articles_resp.root:
                if article.body:
                    filtered_articles.append(
                        {
                            "from": article.from_field or "Unknown",
                            "body": article.body,  # Keep \n for full text
                        }
                    )

            logger.info(
                f"Ticket {ticket_id} | SUCCESS | {len(filtered_articles)} articles fetched"
            )
            return filtered_articles
        except Exception as e:
            logger.error(
                f"Ticket {ticket_id} | FAILED articles fetch | {str(e)} | continuing without articles"
            )
            return []  # Don't raise — continue with other tickets

    def process_day(self, date_str: str) -> List[Dict[str, Any]]:
        tickets = self.fetch_all_tickets_for_date(date_str)
        logger.info(f"Starting articles for {len(tickets)} tickets on {date_str}")

        enriched_tickets = []
        error_count = 0

        for idx, ticket in enumerate(tickets, 1):
            logger.info(
                f"[{idx}/{len(tickets)}] Processing articles for ticket {ticket['id']} | expected: {ticket['article_count']}"
            )

            articles = self.get_articles_for_ticket(ticket["id"])

            enriched = ticket.copy()
            for i, art in enumerate(articles, 1):
                enriched[f"from_{i}"] = art["from"]
                enriched[f"body_{i}"] = art["body"]

            # If no articles but expected >0 — log
            if ticket["article_count"] > 0 and not articles:
                logger.warning(
                    f"Ticket {ticket['id']} | expected {ticket['article_count']} but got 0 articles"
                )

            enriched_tickets.append(enriched)

            # GC every 10 tickets to prevent OOM in prod
            if idx % 10 == 0:
                gc.collect()
                logger.debug(f"GC after {idx} tickets on {date_str}")

        logger.info(
            f"Finished {date_str} | {len(enriched_tickets)} enriched | {error_count} errors"
        )
        gc.collect()  # Final GC
        return enriched_tickets

    def close(self):
        self.client.close()
