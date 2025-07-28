from fastapi import FastAPI, Request
from pydantic import BaseModel
import openai
from openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv
from typing import Optional

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()  # API key is auto-loaded from env
# DB connection
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

app = FastAPI()

# Session memory (simple)
user_context = {}

class QueryRequest(BaseModel):
    question: str
    session_id: Optional[str] = "default"

@app.post("/ask")
def ask_question(request: QueryRequest):
    question = request.question
    session_id = request.session_id

    context = user_context.get(session_id, "")

    prompt = f"""
You are a helpful assistant that converts natural language to SQL and returns database results.

Database Schema:
- doc_metadata(dm_id, dm_ocr_pages, dm_ocr_content, dm_folder_name, dm_site_name)
- sites(site_id, site_name, site_parent_id, site_code, site_prefix, site_record_value)

Examples:
Q: How many documents are in folder XYZ?
A: SELECT COUNT(*) FROM doc_metadata WHERE dm_folder_name = 'XYZ';

Q: List all documents under site PMC.
A: SELECT * FROM doc_metadata WHERE dm_site_name = 'PMC';

Q: How many sites do we have?
A: SELECT COUNT(*) FROM sites;

Context from previous questions: "{context}"

Now, convert this question to SQL:
Q: {question}
A:
"""

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}]
    )

    sql_query = response.choices[0].message.content.strip()

    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            rows = cur.fetchall()

        # Check if content field is being returned
        if "dm_ocr_content" in sql_query.lower():
            doc_text = rows[0][0]
            qa_prompt = f"""You are an intelligent assistant. Use the following document text to answer the question:

        Document:
        \"\"\"
        {doc_text}
        \"\"\"

        Question:
        {question}

        Answer:"""

            answer_response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": qa_prompt}]
            )

            final_answer = answer_response.choices[0].message.content.strip()

            return {
                "sql": sql_query,
                "answer": final_answer
            }

        # Otherwise, just return SQL result
        user_context[session_id] = context + "\n" + question
        return {"sql": sql_query, "result": rows}

    except Exception as e:
        return {"error": str(e), "sql": sql_query}