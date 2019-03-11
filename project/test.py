from input_query import process_input_query

with open("sample_queries/vertex_search.json", "rb") as f:
    query = f.read()
    process_input_query(query)