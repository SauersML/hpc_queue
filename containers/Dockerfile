FROM python:3.12-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG GNOMON_REF=main
ARG REAGLE_REF=main

ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV MAMBA_ROOT_PREFIX=/opt/conda
ENV RUSTUP_HOME=/opt/rust/rustup
ENV CARGO_HOME=/opt/rust/cargo
ENV PATH=/opt/rust/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN apt-get update \
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
        unzip \
        vim \
        wget \
        zip \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSfL https://micro.mamba.pm/api/micromamba/linux-64/latest \
      | tar -xvj -C /usr/local/bin --strip-components=1 bin/micromamba \
    && micromamba create -y -n base -c conda-forge -c bioconda plink2 gctb \
    && ln -sf /opt/conda/envs/base/bin/plink2 /usr/local/bin/plink2 \
    && ln -sf /opt/conda/envs/base/bin/gctb /usr/local/bin/gctb

RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable \
    && rustup update stable \
    && rustup default stable \
    && rustc --version \
    && cargo --version

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

RUN mkdir -p /opt/src \
    && git clone --depth 1 --branch "${GNOMON_REF}" https://github.com/SauersML/gnomon.git /opt/src/gnomon \
    && git clone --depth 1 --branch "${REAGLE_REF}" https://github.com/SauersML/reagle.git /opt/src/reagle

WORKDIR /app
COPY app/run.py /app/run.py
RUN chmod +x /app/run.py

ENTRYPOINT ["python", "/app/run.py"]
