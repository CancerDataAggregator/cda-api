# syntax=docker/dockerfile:1

ARG ALPINE_VERSION="3.24.1"

# Setup builder image to build and install python packages
FROM alpine:${ALPINE_VERSION} AS builder

RUN apk update
RUN apk upgrade
RUN apk add --no-cache pipx

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/go/dockerfile-user-best-practices/
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

WORKDIR /app
COPY . .

ENV PIPX_HOME=/var/lib/pipx
ENV PIPX_BIN_DIR=/usr/local/bin
RUN pipx install poetry

# Tell poetry to create the virtualenv inside the project folder
ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=true \
    POETRY_HOME='/usr/local' \
    POETRY_NO_INTERACTION=1 \
    POETRY_VERSION="2.4.1"
RUN poetry install --no-cache --no-interaction --no-ansi --no-root

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

# Set up environment variable to indicate the app is running in docker
ENV DOCKER_DEPLOYED=1

# Switch to the non-privileged user to run the application.
USER appuser

# Expose the port that the application listens on.
EXPOSE 8000
EXPOSE 5432

# Run the application within the poetry virtual environment
# CMD ["poetry", "run", "fastapi", "run", "cda_api/main.py", "--port", "8000"]
CMD ["poetry", "run", "start_api" ]