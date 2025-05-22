import asyncio
import aiohttp
import aiomysql
from datetime import datetime, timedelta

# Fetch data from the Federal Register API with rate limiting and retries
async def fetch_data(session, url, retries=3, delay=120):
    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if response.status == 429:  # Too Many Requests
                    print(f"Rate limit hit, retrying after {delay} seconds...")
                    await asyncio.sleep(delay)
                    continue
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            if attempt == retries - 1:
                raise Exception(f"Failed to fetch data from {url}: {str(e)}")
            print(f"Error on attempt {attempt + 1}: {str(e)}, retrying after {delay} seconds...")
            await asyncio.sleep(delay)
    raise Exception(f"Failed to fetch data from {url}: Too many retries")

# Store documents in the MySQL database
async def store_documents(pool, documents):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for doc in documents:
                await cur.execute(
                    """
                    INSERT INTO documents (document_number, title, publication_date, document_type, abstract)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title=%s, publication_date=%s, document_type=%s, abstract=%s
                    """,
                    (
                        doc.get("document_number"),
                        doc.get("title"),
                        doc.get("publication_date"),
                        doc.get("type"),
                        doc.get("abstract"),
                        doc.get("title"),
                        doc.get("publication_date"),
                        doc.get("type"),
                        doc.get("abstract"),
                    ),
                )
            await conn.commit()

# Process a single day's data
async def process_day(session, pool, day):
    url = f"https://www.federalregister.gov/api/v1/documents?per_page=1000&conditions[publication_date][is]={day}"
    try:
        data = await fetch_data(session, url)
        documents = data.get("results", [])
        if documents:
            print(f"Fetched {len(documents)} documents for {day}")
            await store_documents(pool, documents)
        else:
            print(f"No documents found for {day}")
    except Exception as e:
        print(f"Error processing {day}: {str(e)}")

# Main function to process documents for May 2025
async def main():
    # Database connection pool
    pool = await aiomysql.create_pool(
        host="localhost",
        port=3306,
        user="root",
        password="Akash@sql#41",
        db="federal_register",
        autocommit=True,
    )

    # HTTP session for fetching data
    async with aiohttp.ClientSession() as session:
        # Process each day in May 2025
        start_date = datetime(2025, 5, 1)
        end_date = datetime(2025, 5, 31)
        tasks = []
        current_date = start_date
        while current_date <= end_date:
            day_str = current_date.strftime("%Y-%m-%d")
            tasks.append(process_day(session, pool, day_str))
            current_date += timedelta(days=1)
        await asyncio.gather(*tasks)

    pool.close()
    await pool.wait_closed()

# Run the pipeline
if __name__ == "__main__":
    asyncio.run(main())
