# syntax=docker/dockerfile:1
# =============================================================================
# GPMorgan Forge: Multi-Stage Production & Verification Container Image
# =============================================================================

FROM python:3.15-slim-bookworm AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    DISABLE_SQLALCHEMY_CEXT=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy dependency definitions first for deterministic layer caching
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src

RUN uv venv /app/.venv && \
    uv pip install --python /app/.venv/bin/python -e ".[dev]"

# -----------------------------------------------------------------------------
# Runtime & Verification Stage
# -----------------------------------------------------------------------------
FROM python:3.15-slim-bookworm AS runtime

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Create non-privileged system group, user, and writable application workspace
RUN groupadd --system --gid 10001 forge && \
    useradd --system --uid 10001 --gid forge --create-home --home-dir /home/forge forge && \
    mkdir -p /app && \
    chown -R forge:forge /app

WORKDIR /app

COPY --from=builder --chown=forge:forge /app/.venv /app/.venv
COPY --chown=forge:forge pyproject.toml uv.lock README.md LICENSE .env.example .gitattributes compose.yaml Dockerfile lefthook.yml ./
COPY --chown=forge:forge .github ./.github
COPY --chown=forge:forge src ./src
COPY --chown=forge:forge tests ./tests
COPY --chown=forge:forge scripts ./scripts
COPY --chown=forge:forge docs ./docs
COPY --chown=forge:forge portfolio-1-database-design ./portfolio-1-database-design
COPY --chown=forge:forge portfolio-2-query-optimisation ./portfolio-2-query-optimisation
COPY --chown=forge:forge portfolio-3-transactions-concurrency ./portfolio-3-transactions-concurrency
COPY --chown=forge:forge portfolio-4-nosql ./portfolio-4-nosql
COPY --chown=forge:forge portfolio-5-distributed-cloud ./portfolio-5-distributed-cloud

USER forge

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import gpmorgan_forge; print(gpmorgan_forge.__version__)"]

CMD ["pytest", "--cov=src/gpmorgan_forge", "--cov-branch", "--cov-report=term-missing", "--cov-fail-under=100"]
