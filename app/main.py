from fastapi import FastAPI, HTTPException, Query
from datetime import datetime, timedelta
from typing import Dict, Any, List
import os
from .settings import settings
from .schemas import TicketQuery
from .services.zammad_client import ZammadClient
from .utils.csv_writer import write_tickets_to_csv
from .app_logger import logger

app = FastAPI(
    title=settings.title,
    description=settings.description,
    version=settings.version,
    debug=settings.debug,
)

# Global client instance; shared across requests (razovaya operation, but safe)
client = ZammadClient()


@app.on_event("shutdown")
def shutdown_event():
    """Cleanup on app shutdown."""
    client.close()
    logger.info("App shutdown: client closed")


@app.get("/get_ticket_data")
async def get_ticket_data(
    start_date: str = Query(
        ..., description="Start date in YYYY-MM-DD format", example="2025-10-09"
    ),
    end_date: str = Query(
        ..., description="End date in YYYY-MM-DD format", example="2025-10-10"
    ),
):
    """
    Main endpoint: GET /get_ticket_data?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
    Parameters are query params (not body) for filtering date range.
    Validates dates via Pydantic (integrated in TicketQuery).
    Processes range day-by-day, enriches with articles, appends to CSV.
    Returns summary; logs all steps/errors.

    OpenAPI/Swagger: Visit /docs to see examples and validation.
    """
    try:
        # Create TicketQuery instance for validation (Pydantic handles it)
        query = TicketQuery(start_date=start_date, end_date=end_date)

        # Dates already validated by Pydantic in TicketQuery
        start = datetime.strptime(query.start_date, "%Y-%m-%d")
        end = datetime.strptime(query.end_date, "%Y-%m-%d")

        all_data: List[Dict[str, Any]] = []
        current = start
        total_processed = 0

        logger.info(
            f"Starting ticket fetch for range {query.start_date} to {query.end_date}"
        )

        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            logger.info(f"Processing day: {date_str}")

            day_tickets = client.process_day(date_str)
            all_data.extend(day_tickets)
            total_processed += len(day_tickets)

            current += timedelta(days=1)

        # Append to CSV
        write_tickets_to_csv(all_data)

        return {
            "status": "success",
            "total_tickets_processed": total_processed,
            "csv_appended_to": os.path.abspath(settings.csv_path),
            "date_range": f"{query.start_date} to {query.end_date}",
        }

    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in get_ticket_data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Internal server error during processing"
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", reload=settings.debug, host="0.0.0.0", port=8000)
    # Comment: Use poetry run python main.py; or uvicorn directly
