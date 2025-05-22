async function submitQuery() {
    const queryInput = document.getElementById("queryInput").value.trim();
    const resultsDiv = document.getElementById("results");

    if (!queryInput) {
        resultsDiv.innerText = "Please enter a query.";
        return;
    }

    resultsDiv.innerText = "Loading...";

    try {
        const response = await fetch(`http://localhost:8000/query/${encodeURIComponent(queryInput)}`);
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }
        const data = await response.json();
        resultsDiv.innerText = `Query: ${data.query}\n\nResponse:\n${data.response}`;
    } catch (error) {
        resultsDiv.innerText = `Error: ${error.message}`;
    }
}

// Allow pressing Enter to submit the query
document.getElementById("queryInput").addEventListener("keypress", function (event) {
    if (event.key === "Enter") {
        submitQuery();
    }
});