import asyncio
import subprocess
from google.cloud import storage, bigquery


async def download_and_upload_to_gcs(url, bucket_name, blob_name):
    try:
        # Download the file using curl and write to tmp file
        subprocess.run(["curl", "-L", url, "--output", f"/tmp/{blob_name}"])
        print("Downloading -> {url}")

        # Upload tmp file to Google Cloud Storage
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(f"/tmp/{blob_name}")

        print(f"\tDone ->")
    except Exception as e:
        print(f"\tError: {e}")


async def load_to_bigquery(gcs_uri, dataset_id, table_id):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    print("\tLoading to BigQuery")
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="\t",
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()

    print("\tDone!")


async def process_url(url, gcs_bucket_name):
    blob_name = url.split("/")[-1]
    await download_and_upload_to_gcs(url, gcs_bucket_name, blob_name)
    await load_to_bigquery(
        f"gs://{gcs_bucket_name}/{blob_name}",
        "raw_us",
        blob_name.split(".")[0].replace("-", "_"),
    )


async def main():
    gcs_bucket_name = "iota-inbound-p0e"

    tasks = [process_url(url, gcs_bucket_name) for url in urls]
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    urls = [
        "https://www3.cde.ca.gov/researchfiles/cadashboard/eladownload2021.txt",
        # "https://www3.cde.ca.gov/researchfiles/cadashboard/eladownload2023.txt",
        # "https://www3.cde.ca.gov/researchfiles/cadashboard/eladownload2022.txt",
        # "https://www3.cde.ca.gov/demo-downloads/acgr/acgr23.txt",
        # "https://www3.cde.ca.gov/demo-downloads/acgr/acgr22-v3.txt",
    ]

    asyncio.run(main())
