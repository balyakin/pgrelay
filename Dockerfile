FROM python:3.12-slim AS builder

WORKDIR /app
RUN python -m pip install --upgrade pip==25.1.1 && python -m pip install poetry==2.1.3
COPY pyproject.toml poetry.lock README.md LICENSE alembic.ini ./
COPY migrations ./migrations
COPY src ./src
RUN poetry build -f wheel

FROM python:3.12-slim AS runtime

WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY --from=builder /app/dist/*.whl /tmp/
RUN python -m pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl
EXPOSE 8090
ENTRYPOINT ["pgrelay"]
CMD ["api"]
