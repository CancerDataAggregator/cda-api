# syntax=docker/dockerfile:1

ARG ALPINE_VERSION="3.21.3"

FROM alpine:${ALPINE_VERSION}

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

# Install Python
ARG PYTHON_VERSION="3.12"
RUN apk add --update --no-cache python3=~${PYTHON_VERSION} py3-pip py3-setuptools pipx

# Install prereqs for python packages
RUN apk add gcc python3-dev musl-dev linux-headers

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

# Set environment variables for poetry and install with pipx
ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=true \
    POETRY_HOME='/usr/local' \
    POETRY_NO_INTERACTION=1 \
    POETRY_VERSION="1.8.3"
RUN pipx install poetry==${POETRY_VERSION} --global

# Change to app directory
WORKDIR /app

# copy only pyproject.toml nothing else here
COPY pyproject.toml ./

# This will create the folder /app/.venv
RUN poetry install --no-root

# Switch to the non-privileged user to run the application.
USER appuser

# Copy the source code into the container.
COPY . .

# Expose the port that the application listens on.
EXPOSE 8000
EXPOSE 5432

# Set up environment variable to indicate the app is running in docker
ENV DOCKER_DEPLOYED=1

# Run the application within the poetry virtual environment
CMD ["poetry", "run", "fastapi", "run", "cda_api/main.py", "--port", "8000"]

