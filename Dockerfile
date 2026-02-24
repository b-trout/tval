ARG UV_VERSION=0.6
ARG PYTHON_VERSION=3.12
ARG DEBIAN_VERSION=trixie-slim

FROM ghcr.io/astral-sh/uv:${UV_VERSION}-python${PYTHON_VERSION}-${DEBIAN_VERSION}

# Install sudo
RUN apt-get update \
    && apt-get install -y --no-install-recommends sudo \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user with sudo privileges
ARG USERNAME=dev
ARG UID=1000
ARG GID=1000
RUN groupadd -g ${GID} ${USERNAME} \
    && useradd -m -u ${UID} -g ${GID} -s /bin/bash ${USERNAME} \
    && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME}

USER ${USERNAME}
WORKDIR /home/${USERNAME}/workspace

# Create virtual environment and install dependencies
COPY pyproject.toml .
RUN uv sync --group dev
ENV VIRTUAL_ENV=/home/${USERNAME}/workspace/.venv
ENV PATH="/home/${USERNAME}/workspace/.venv/bin:${PATH}"
