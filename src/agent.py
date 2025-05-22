import asyncio
import aiomysql
from openai import AsyncOpenAI
from typing import List, Dict, Any

# Initialize Ollama client (using OpenAI-compatible API)
client = AsyncOpenAI(base_url="http://localhost:11434/v1", api_key="ollama")


async def retrieve_documents(query: str, limit: int = 5) -> List[Dict[str, Any]]:
    pool = await aiomysql.create_pool(
        host="localhost", user="root", password="Akash@sql#41", db="federal_register", autocommit=True
    )
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            # Simple keyword search (can be enhanced with full-text search or embeddings)
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