FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl git sudo redis-tools psmisc \
       docker.io docker-compose-v2 \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL https://go.dev/dl/go1.26.0.linux-amd64.tar.gz \
       | tar -C /usr/local -xzf - \
    && groupadd -f docker \
    && useradd -m -s /bin/bash -G sudo,docker ubuntu || true \
    && usermod -aG docker ubuntu 2>/dev/null || true

ENV PATH="/usr/local/go/bin:${PATH}"

USER ubuntu
WORKDIR /home/ubuntu
