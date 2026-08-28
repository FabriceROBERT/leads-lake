# tippecanoe (built from source — no public image exists) + a small Python
# runtime to read Gold and push the .pmtiles archive.
FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      git g++ make libsqlite3-dev zlib1g-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
 && make -C /tmp/tippecanoe -j 4 \
 && make -C /tmp/tippecanoe install \
 && rm -rf /tmp/tippecanoe

RUN pip install --no-cache-dir \
      "pandas==2.2.3" "pyarrow==18.1.0" "s3fs==2025.10.0"

COPY jobs/build_tiles.py /opt/build_tiles.py

ENTRYPOINT ["python", "/opt/build_tiles.py"]
