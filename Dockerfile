ARG UV_VERSION=0.10
ARG PYTHON_VERSION=3.12
ARG DEBIAN_VERSION=trixie-slim

FROM ghcr.io/astral-sh/uv:${UV_VERSION}-python${PYTHON_VERSION}-${DEBIAN_VERSION}

# Install sudo and git
RUN apt-get update \
    && apt-get install -y --no-install-recommends sudo git \
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
COPY pyproject.toml README.md ./
COPY src/tval/__init__.py src/tval/__init__.py
COPY .pre-commit-config.yaml .
RUN SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 uv sync --extra dev --no-install-project
ENV VIRTUAL_ENV=/home/${USERNAME}/workspace/.venv
ENV PATH="/home/${USERNAME}/workspace/.venv/bin:${PATH}"

# Install pre-commit hook
RUN git init /home/${USERNAME}/workspace 2>/dev/null; \
    pre-commit install
