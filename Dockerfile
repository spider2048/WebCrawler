FROM python:slim

WORKDIR /app

COPY crawler /app/ 
COPY models /app/
COPY search_engine /app/
COPY crawl.toml pyproject.toml poetry.lock /app/

ENV YOUR_ENV=${YOUR_ENV} \
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_NO_INTERACTION=1 \
  POETRY_VIRTUALENVS_CREATE=false \
  PATH=${PATH}:/root/.local/bin

RUN apt-get update && apt-get install -y curl build-essential
RUN curl -sSL https://install.python-poetry.org | python3 -

RUN poetry install
