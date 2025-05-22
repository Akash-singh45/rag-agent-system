import asyncio
import aiomysql
from openai import AsyncOpenAI
from typing import List, Dict, Any
import re
from datetime import datetime

# Initialize Ollama client (using OpenAI-compatible API)
client = AsyncOpenAI(base_url="http://localhost:11435/v1", api_key="ollama")


def parse_date_from_query(query: str) -> str:
    # Regular expression to match dates like "May 22 2025" or "2025-05-22"
    date_pattern = r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}\s+\d{4}\b|\b\d{4}-\d{2}-\d{2}\b'
    match = re.search(date_pattern, query, re.IGNORECASE)
    if match:
        date_str = match.group(0)
        # Convert "May 22 2025" to "2025-05-22"
        try:
            if "-" in date_str:
                return date_str
            date_obj = datetime.strptime(date_str, "%B %d %Y")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


async def retrieve_documents(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    pool = await aiomysql.create_pool(
        host="localhost", user="root", password="Akash@sql#41", db="federal_register", autocommit=True
    )
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Extract date from query
            date = parse_date_from_query(query)

            if date:
                # If a date is found, search by publication_date
                await cur.execute(
                    """
                    SELECT document_number, title, publication_date, abstract, agencies
                    FROM documents
                    WHERE publication_date = %s
                    LIMIT %s
                    """,
                    (date, limit)
                )
            else:
                # Otherwise, fall back to searching title or abstract
                await cur.execute(
                    """
                    SELECT document_number, title, publication_date, abstract, agencies
                    FROM documents
                    WHERE title LIKE %s OR abstract LIKE %s
                    LIMIT %s
                    """,
                    (f"%{query}%", f"%{query}%", limit)
                )
            results = await cur.fetchall()
    pool.close()
    await pool.wait_closed()
    return results


async def generate_response(query: str, documents: List[Dict[str, Any]]) -> str:
    # Prepare context from retrieved documents
    context = ""
    for doc in documents:
        context += f"Document Number: {doc['document_number']}\n"
        context += f"Title: {doc['title']}\n"
        context += f"Publication Date: {doc['publication_date']}\n"
        context += f"Abstract: {doc['abstract']}\n"
        context += f"Agencies: {doc['agencies']}\n\n"

    # Create the prompt for the LLM
    prompt = f"""
    You are a helpful assistant that answers questions based on Federal Register documents.
    Below is the user's query and relevant documents retrieved from the Federal Register database.
    Use the documents to provide an accurate and concise answer to the query. If the documents
    do not contain enough information to answer the query, say so and provide any relevant
    information you can.

    ### User Query:
    {query}

    ### Relevant Documents:
    {context}

    ### Answer:
    """

    # Call the Ollama model (Qwen2.5)
    response = await client.chat.completions.create(
        model="qwen2.5:0.5b",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=500,
        temperature=0.7,
    )

    return response.choices[0].message.content.strip()


async def rag_agent(query: str) -> str:
    # Step 1: Retrieve relevant documents
    documents = await retrieve_documents(query, limit=5)

    # Step 2: Generate response using the documents
    if not documents:
        return "No relevant documents found for your query."

    response = await generate_response(query, documents)
    return response


# Example usage
async def main():
    query = "List executive orders from May 2025"
    response = await rag_agent(query)
    print(response)


if __name__ == "__main__":
    asyncio.run(main())