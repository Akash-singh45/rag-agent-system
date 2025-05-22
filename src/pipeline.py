import asyncio
import aiohttp
import aiomysql
import json
from datetime import datetime, timedelta
from pathlib import Path

# Define paths for raw and processed data
RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)


async def fetch_data(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            raise Exception(f"Failed to fetch data from {url}: {response.status}")
        return await response.json()


async def save_to_mysql(data):
    pool = await aiomysql.create_pool(
        host="localhost", user="root", password="Akash@sql#41", db="federal_register", autocommit=True
    )
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for doc in data:
                document_number = doc.get("document_number")
                title = doc.get("title")
                publication_date = doc.get("publication_date")
                abstract = doc.get("abstract")
                agencies = ", ".join(agency["name"] for agency in doc.get("agencies", []))

                await cur.execute(
                    """
                    INSERT INTO documents (document_number, title, publication_date, abstract, agencies)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title = VALUES(title),
                        publication_date = VALUES(publication_date),
                        abstract = VALUES(abstract),
                        agencies = VALUES(agencies)
                    """,
                    (document_number, title, publication_date, abstract, agencies)
                )
    pool.close()
    await pool.wait_closed()


async def process_day(session, date):
    date_str = date.strftime("%Y-%m-%d")
    raw_file = RAW_DATA_DIR / f"{date_str}.json"
    processed_file = PROCESSED_DATA_DIR / f"{date_str}.json"

    # Fetch data if raw file doesn't exist
    if not raw_file.exists():
        url = f"https://www.federalregister.gov/api/v1/documents?per_page=1000&conditions[publication_date][is]={date_str}"
        data = await fetch_data(session, url)
        with open(raw_file, "w") as f:
            json.dump(data, f, indent=2)

    # Process data
    with open(raw_file) as f:
        raw_data = json.load(f)

    processed_data = []
    for doc in raw_data.get("results", []):
        processed_doc = {
            "document_number": doc.get("document_number"),
            "title": doc.get("title"),
            "publication_date": doc.get("publication_date"),
            "abstract": doc.get("abstract"),
            "agencies": doc.get("agencies", []),
        }
        processed_data.append(processed_doc)

    # Save processed data to file
    with open(processed_file, "w") as f:
        json.dump(processed_data, f, indent=2)

    # Save to MySQL
    await save_to_mysql(processed_data)


async def main():
    # Fetch data for the last 60 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=60)

    async with aiohttp.ClientSession() as session:
        tasks = []
        current_date = start_date
        while current_date <= end_date:
            tasks.append(process_day(session, current_date))
            current_date += timedelta(days=1)
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())