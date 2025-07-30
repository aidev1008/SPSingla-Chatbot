from fastapi import FastAPI
from pydantic import BaseModel
import openai
from openai import OpenAI
import psycopg2
import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

# FastAPI app
app = FastAPI()

# In-memory session context
user_context = {}

# Pydantic request model
class QueryRequest(BaseModel):
    question: str
    session_id: Optional[str] = "default"

@app.post("/ask")
def ask_question(request: QueryRequest):
    question = request.question
    session_id = request.session_id

    # Use previous context if exists
    context = user_context.get(session_id, "")

    prompt = f"""
    You are a helpful assistant that converts natural language to SQL and returns database results.

    Database Schema:
    - doc_metadata(dm_id, dm_ocr_pages, dm_ocr_content, dm_folder_name, dm_site_name)
    - sites(site_id, site_name, site_parent_id, site_code, site_prefix, site_record_value)
    - documents(
        doc_id, doc_type, doc_number, doc_purpose, doc_storage_location,
        doc_uploaded_by, doc_site, doc_reference, doc_confidential, doc_from,
        doc_to, doc_source, doc_folder, doc_subject, doc_pdf_link,
        doc_ocr_processed, doc_created_at, doc_status, doc_uploaded_by_id,
        doc_uploaded_at, doc_reply_required, doc_replied_vide,
        doc_ocr_status, doc_history_junction_id
    )

    Examples:
    Q: How many documents are in folder XYZ?
    A: SELECT COUNT(*) FROM doc_metadata WHERE dm_folder_name = 'XYZ';

    Q: List all documents under site PMC.
    A: SELECT * FROM doc_metadata WHERE dm_site_name = 'PMC';

    Q: Who is the person mentioned in the document PMC/05/BBB/C-1/2024?
    A: SELECT dm_ocr_content FROM doc_metadata WHERE dm_id = 'PMC/05/BBB/C-1/2024';

    Q: I need the PDF link for document SPS/P-353/15.
    A: SELECT doc_pdf_link FROM documents WHERE doc_number = 'SPS/P-353/15';

    Q: Show all outgoing documents.
    A: SELECT * FROM documents WHERE doc_type = 'OUTGOING';

    Q: List documents uploaded by Dinesh Kumar.
    A: SELECT * FROM documents WHERE doc_uploaded_by ILIKE '%Dinesh Kumar%';

    Q: How many documents were uploaded from site SPS Digah?
    A: SELECT COUNT(*) FROM documents WHERE doc_site ILIKE '%SPS Digah%';

    Context from previous questions: "{context}"

    Now, convert this question to SQL:
    Q: {question}
    A:
    """

    try:
        # Generate SQL using GPT
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        sql_query = response.choices[0].message.content.strip()

        with conn.cursor() as cur:
            cur.execute(sql_query)
            rows = cur.fetchall()

        # Save session context
        user_context[session_id] = context + "\n" + question

        if "dm_ocr_content" in sql_query.lower():
            if not rows:
                return {"error": "No matching document found", "sql": sql_query}
            doc_text = rows[0][0]
            qa_prompt = f"""You are an intelligent assistant. Use the following document text to answer the question:

            Document:
            \"\"\"{doc_text}\"\"\"

            Question: {question}
            Answer:"""

            answer_response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": qa_prompt}]
            )
            final_answer = answer_response.choices[0].message.content.strip()

            return {"sql": sql_query, "answer": final_answer}

        return {"sql": sql_query, "result": rows}

    except Exception as e:
        conn.rollback()  # 🔥 ADD THIS LINE to clear failed transaction
        return {"error": str(e), "sql": sql_query}