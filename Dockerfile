FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    LINKEDIN_CLI_HOME=/data

WORKDIR /opt/linkedin-cli

COPY pyproject.toml README.md LICENSE ./
COPY linkedin_cli ./linkedin_cli
COPY scripts ./scripts

RUN pip install --upgrade pip && pip install .

WORKDIR /workspace

ENTRYPOINT ["python", "-m", "linkedin_cli"]
CMD ["--help"]
