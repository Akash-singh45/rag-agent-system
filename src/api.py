from fastapi import FastAPI
import aiomysql
from src.agent import rag_agent

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # Initialize MySQL connection pool
    app.state.pool = await aiomysql.create_pool(
        host="localhost", user="root", password="Akash@sql#41", db="federal_register", autocommit=True
    )

@app.on_event("shutdown")
async def shutdown_event():
    # Close MySQL connection pool
    app.state.pool.close()
    await app.state.pool.wait_closed()

@app.get("/query/{query}")
async def query_endpoint(query: str):
    # Call the RAG agent to process the query
    response = await rag_agent(query)
    return {"query": query, "response": response}

# Example usage: Run with `uvicorn src.api:app --reload`
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)