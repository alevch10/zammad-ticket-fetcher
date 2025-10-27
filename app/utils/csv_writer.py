import pandas as pd
import os
from typing import List, Dict, Any
from ..settings import settings
from ..app_logger import logger


def write_tickets_to_csv(tickets: List[Dict[str, Any]]):
    """
    Append enriched tickets to CSV at settings.csv_path.
    Dynamically determines columns from data (base + from_{i}/body_{i} up to max in batch).
    If file exists, append without header; else create with header.
    Handles variable article counts with empty strings for missing.
    """
    if not tickets:
        logger.warning("No tickets to append to CSV")
        return

    csv_path = settings.csv_path
    logger.info(f"Preparing to append {len(tickets)} tickets to {csv_path}")

    # Find max articles in this batch for columns
    max_articles = max(
        (len([k for k in t if k.startswith("from_")]) for t in tickets), default=0
    )
    logger.info(
        f"Max articles in batch: {max_articles}, creating columns up to {max_articles}"
    )

    # Prepare data rows
    data = []
    for ticket in tickets:
        row = {
            "id": ticket["id"],
            "state": ticket["state"],
            "title": ticket["title"],
            "article_count": ticket["article_count"],
        }
        # Add all from/body from this ticket (may be less than max)
        for i in range(1, max_articles + 1):
            row[f"from_{i}"] = ticket.get(f"from_{i}", "")  # Empty if missing
            row[f"body_{i}"] = ticket.get(f"body_{i}", "")
        data.append(row)

    # Create DF
    df = pd.DataFrame(data)

    # Append logic
    if os.path.exists(csv_path):
        # Append without header/index
        df.to_csv(csv_path, mode="a", header=False, index=False, encoding="utf-8")
        logger.info(f"Appended {len(data)} rows to existing CSV {csv_path}")
    else:
        # Create new with header
        df.to_csv(csv_path, mode="w", header=True, index=False, encoding="utf-8")
        logger.info(f"Created new CSV {csv_path} with {len(data)} rows")

    # Comment: For very large appends, consider chunking df.to_csv(chunksize=1000); extend here if needed
