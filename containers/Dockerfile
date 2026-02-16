# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV MAMBA_ROOT_PREFIX=/opt/conda
ENV MAMBA_TOOLS_ENV=/opt/conda/envs/tools
ENV RUSTUP_HOME=/opt/rust/rustup
ENV CARGO_HOME=/opt/rust/cargo
ENV PATH=/opt/conda/bin:/opt/conda/envs/tools/bin:/opt/rust/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends \
        bcftools \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        file \
        g++ \
        gcc \
        git \
        gpg \
        htop \
        jq \
        less \
        make \
        pkg-config \
        procps \
        time \
        unzip \
        vim \
        wget \
        zip \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/opt/conda/pkgs,sharing=locked \
    curl -sSfL https://micro.mamba.pm/api/micromamba/linux-64/latest \
      | tar -xvj -C /usr/local/bin --strip-components=1 bin/micromamba \
    && micromamba create -y -p "${MAMBA_TOOLS_ENV}" -c conda-forge -c bioconda plink2 gctb

RUN set -eux; \
    wget -q https://s3.amazonaws.com/plink2-assets/plink2_linux_avx2_20260110.zip -O /tmp/plink2.zip \
      || wget -q https://s3.amazonaws.com/plink2-assets/plink2_linux_x86_64_20260110.zip -O /tmp/plink2.zip; \
    rm -rf /tmp/plink2 && mkdir -p /tmp/plink2; \
    unzip -o /tmp/plink2.zip -d /tmp/plink2; \
    install -m 0755 /tmp/plink2/plink2 /usr/bin/plink2; \
    rm -rf /tmp/plink2 /tmp/plink2.zip

RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable \
    && rustup update stable \
    && rustup default stable \
    && rustc --version \
    && cargo --version

COPY requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install -r /tmp/requirements.txt

RUN mkdir -p /gnomon /reagle

RUN set -eux; \
    command -v bcftools; \
    test -x /usr/bin/time; \
    test -x /usr/bin/plink2; \
    if command -v gctb >/dev/null 2>&1; then \
      install -m 0755 "$(command -v gctb)" /usr/bin/gctb; \
    elif command -v gctb64 >/dev/null 2>&1; then \
      install -m 0755 "$(command -v gctb64)" /usr/bin/gctb; \
    else \
      echo "gctb binary not found after install" >&2; \
      exit 1; \
    fi; \
    test -x /usr/bin/gctb

WORKDIR /app
COPY app/run.py /app/run.py
RUN chmod +x /app/run.py

ENTRYPOINT ["python", "/app/run.py"]
