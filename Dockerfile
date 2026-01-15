# Start with slim Python 3.13 image for smaller size
FROM python:3.13.11-slim

# Copy uv binary from official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory inside container
WORKDIR /app

# Add virtual environment to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files first (better caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install all dependencies (pandas, sqlalchemy, psycopg2)
RUN uv sync --locked

# Copy ingestion script
COPY nyc_taxi_postres.py ingest_data.py 

# Set entry point to run the ingestion script
ENTRYPOINT [ "python", "ingest_data.py" ]


docker run -it /
--network=pg-network / 
docker.io/amynsfwt/pg-ingest-table:V1.0.1 /
--prefix https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow /
--year 2021/
 --month 1 /
 --pg-user root /
 --pg-password root /
 --pg-host friendly_moore /
 --pg-port 5434 --pg-db nyc_taxi /
 --chunksize 100000 /
 --target-table yellow_taxi_data_2021 