"""
This module provides functionality to download datasets from specified URLs and save them to a local directory.
The main function in this module downloads datasets from a list of URLs, displays the URLs being downloaded,
creates the necessary directory structure if it does not exist, and saves the downloaded datasets to the specified
directory. It also logs the progress and any errors encountered during the download process.
Modules:
    - pathlib: Provides object-oriented filesystem paths.
    - urllib.parse: Provides functions to parse URLs.
    - requests: Allows sending HTTP requests.
    - typer: A library for building command-line interface applications.
    - loguru: A library for logging.
    - tqdm: A library for progress bars.
Functions:
    - main: Downloads datasets from specified URLs and saves them to a local directory.
"""

from pathlib import Path
from urllib.parse import urlparse

import requests
import typer
from loguru import logger
from tqdm import tqdm

from capstone_project_1.config import EXTERNAL_DATA_DIR

app = typer.Typer()


@app.command()
def main(**urls):
    """Download the datasets"""

    urls = [
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2024-11.parquet",
        "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-11.parquet",
    ]

    # Display the URLs to download
    url_message = "\n"
    for idx, url in enumerate(urls):
        url_message += f"{idx+1}. {url}\n"
    logger.info(f"Downloading datasets: {url_message}")

    try:
        # Create the external data "ny-taxi-data" directory if it does not exist
        path = Path(EXTERNAL_DATA_DIR / "ny-taxi-data")
        if not path.exists():
            path.mkdir(parents=True)
            logger.info(f"Created directory: {path}")

        for url in tqdm(urls, desc="Downloading datasets"):
            # Download the dataset from the URL
            try:
                response = requests.get(url, stream=True, timeout=60)
                response.raise_for_status()
            except KeyboardInterrupt:
                raise
            except requests.exceptions.HTTPError as e:
                logger.error(f"Failed to download dataset from {url}")
                logger.error(e)

            else:
                logger.info(f"Downloading dataset from {url}")

            # Save the dataset to the external data directory "ny-taxi-data"
            try:
                filename = Path(urlparse(url).path).name
                with open(path / filename, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"Failed to save dataset from {url}")
                logger.error(e)
            else:
                logger.success(f"Saved dataset from {url}")

    except KeyboardInterrupt:
        logger.error("Download cancelled.")
    else:
        logger.success("Dataset download complete.")


if __name__ == "__main__":
    app()
