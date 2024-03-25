import asyncio
import time
import aiohttp
import os
import shutil
import pandas as pd
import pyarrow.parquet as pq
import duckdb
from google.cloud import storage
import torch
from transformers import BertTokenizer, BertModel
from fastapi import FastAPI, UploadFile, HTTPException
from typing import List
import logging
from bs4 import BeautifulSoup
import requests

app = FastAPI()

TEMP_DIR: str = "/tmp/extracted_files"
os.makedirs(TEMP_DIR, exist_ok=True)

# Initialize the BERT tokenizer and model
tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
model = BertModel.from_pretrained("bert-base-uncased")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


def get_bert_embedding(text: str) -> List[float]:
    inputs = tokenizer(text, return_tensors="pt", max_length=512, truncation=True)
    with torch.no_grad():
        outputs = model(**inputs)
    embeddings = torch.mean(outputs.last_hidden_state, dim=1)
    return embeddings.tolist()


async def save_uploaded_file(file: UploadFile) -> str:
    file_path = os.path.join(TEMP_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    return file_path


async def download_file(file_url: str, destination: str) -> None:
    async with aiohttp.ClientSession() as session:
        async with session.get(file_url) as response:
            if response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Failed to download file from {file_url}",
                )
            with open(destination, "wb") as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)


async def extract_text_from_pdf(pdf_path: str) -> str:
    return pdf_path

async def extract_text_from_txt(txt_path: str) -> str:
    with open(txt_path, "r") as file:
        return file.read()


async def extract_text_from_csv(csv_path: str) -> str:
    # Load CSV file into DataFrame
    df = pd.read_csv(csv_path)
    # Convert DataFrame to string
    return df.to_string()


async def extract_data_from_parquet(parquet_path: str) -> str:
    # Read parquet file
    table = pq.read_table(parquet_path)
    # Convert table to DataFrame
    df = table.to_pandas()
    # Convert DataFrame to string
    return df.to_string()


async def update_metadata_with_duckdb(metadata: List[dict]) -> None:
    # Convert metadata to DataFrame
    df = pd.DataFrame(metadata)

    # Connect to DuckDB in-memory database
    conn = duckdb.connect(":memory:")

    # Write DataFrame to DuckDB
    conn.register("metadata", df)

    # Execute SQL query to update metadata
    conn.execute("CREATE TABLE metadata AS SELECT * FROM metadata")
    conn.execute(
        "ALTER TABLE metadata ADD COLUMN embedding STRING"
    )  # Add embedding column

    # Update embedding column with BERT embeddings
    for idx, row in enumerate(df.itertuples(), 1):
        embedding = get_bert_embedding(str(row))
        embedding_str = ",".join(map(str, embedding))
        conn.execute(
            f"UPDATE metadata SET embedding = '{embedding_str}' WHERE index = {idx}"
        )

    # Export updated metadata to JSON
    updated_metadata = conn.execute("SELECT * FROM metadata").fetchdf()
    updated_metadata.to_json("metadata.json", orient="records")

    # Print updated metadata to console
    print(updated_metadata)

    # Close connection
    conn.close()


@app.post("/extract/")
async def extract(web_location: str, gcs_bucket_name: str, prefix: str) -> str:
    response = requests.get(web_location)
    if response.status_code != 200:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch web location: {web_location}"
        )

    content_type = response.headers.get("Content-Type", "").lower()
    if "pdf" in content_type:
        text = await extract_text_from_pdf(response.content)
    elif "text" in content_type:
        text = await extract_text_from_txt(response.content)
    elif "csv" in content_type:
        text = await extract_text_from_csv(response.content)
    elif "parquet" in content_type:
        text = await extract_data_from_parquet(response.content)
    else:
        raise HTTPException(
            status_code=400, detail=f"Unsupported content type: {content_type}"
        )

    # Move the file to Google Cloud Storage
    file_name = os.path.basename(web_location)
    file_path = os.path.join(TEMP_DIR, file_name)
    with open(file_path, "wb") as f:
        f.write(response.content)

    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket_name)
    blob = bucket.blob(f"{prefix}/{file_name}")
    blob.upload_from_filename(file_path)

    # Create embedding of the file
    # Assuming you have a function get_embedding(text: str) that returns the embedding
    embedding = get_bert_embedding(text)

    # Move the embedding to Google Cloud Storage
    embedding_path = os.path.join(TEMP_DIR, f"{file_name}.embedding")
    with open(embedding_path, "w") as f:
        f.write(embedding)

    blob = bucket.blob(f"{prefix}/{file_name}.embedding")
    blob.upload_from_filename(embedding_path)

    # Clean up the temporary directory
    os.remove(file_path)
    os.remove(embedding_path)

    return text


@app.post("/extract/")
async def extract_endpoint(web_location: str, gcs_bucket_name: str, prefix: str) -> str:
    return await extract(web_location, gcs_bucket_name, prefix)


async def download_pdf(pdf_url: str) -> None:
    pdf_filename = os.path.basename(pdf_url)
    pdf_path = os.path.join(TEMP_DIR, pdf_filename)
    logging.info(f"Downloading {pdf_filename}...")
    start_time = time.time()
    async with aiohttp.ClientSession() as session:
        async with session.get(pdf_url) as response:
            with open(pdf_path, "wb") as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
    end_time = time.time()
    logging.info(f"Downloaded {pdf_filename} in {end_time - start_time:.2f} seconds")


async def download_pdfs(pdf_urls: List[str]) -> None:
    logging.info(f"Downloading {len(pdf_urls)} PDFs...")
    await asyncio.gather(*(download_pdf(url) for url in pdf_urls))


def list_pdf_files() -> List[str]:
    return [filename for filename in os.listdir(TEMP_DIR) if filename.endswith(".pdf")]


async def upload_pdf(filename: str, gcs_bucket_name: str, prefix: str) -> None:
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket_name)
    blob = bucket.blob(f"{prefix}/{filename}")
    logging.info(f"Uploading {filename}...")
    start_time = time.time()
    blob.upload_from_filename(os.path.join(TEMP_DIR, filename))
    end_time = time.time()
    logging.info(f"Uploaded {filename} in {end_time - start_time:.2f} seconds")


async def upload_to_gcs(
    pdf_filenames: List[str], gcs_bucket_name: str, prefix: str
) -> None:
    logging.info(f"Uploading {len(pdf_filenames)} PDFs to Google Cloud Storage...")
    await asyncio.gather(
        *(upload_pdf(filename, gcs_bucket_name, prefix) for filename in pdf_filenames)
    )


async def cleanup_temporary_directory(file_paths: List[str]) -> None:
    for file_path in file_paths:
        os.remove(file_path)
    logging.info("Temporary directory cleaned up")


@app.post("/process-pdfs/")
async def process_pdfs(web_location: str, gcs_bucket_name: str, prefix: str) -> None:
    # Create temporary directory if it doesn't exist
    os.makedirs(TEMP_DIR, exist_ok=True)
    response = requests.get(web_location)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Failed to fetch web location")

    content_type = response.headers.get("Content-Type", "").lower()
    if "pdf" in content_type:
        pdf_urls = [web_location]
    else:
        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a", href=True)
        pdf_urls = [
            web_location + link["href"]
            for link in links
            if link["href"].endswith(".pdf")
        ]

    await download_pdfs(pdf_urls)
    pdf_filenames = list_pdf_files()
    await upload_to_gcs(pdf_filenames, gcs_bucket_name, prefix)
    await cleanup_temporary_directory(pdf_filenames)
    logging.info("PDF download and upload process completed")
    return {"message": "PDF download and upload process completed successfully"}
