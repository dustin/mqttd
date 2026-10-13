# Stage 1: Build the Haskell project
# The official haskell images have no GHC 9.6 on bookworm, and some
# dependencies (cleff) don't support newer GHCs yet, so install 9.6 via ghcup.
FROM debian:bookworm AS builder

RUN apt-get update && apt-get install -y build-essential curl git libffi-dev libgmp-dev libncurses-dev \
    ffmpeg libpq5 libpq-dev zlib1g zlib1g-dev

ENV PATH=/root/.ghcup/bin:$PATH
RUN curl -sSfL https://downloads.haskell.org/~ghcup/x86_64-linux-ghcup -o /usr/local/bin/ghcup \
    && chmod +x /usr/local/bin/ghcup \
    && ghcup install ghc 9.6.7 --set \
    && ghcup install cabal 3.12.1.0 --set

WORKDIR /usr/src/app

COPY mqttd.cabal ./
RUN cabal update && cabal build --only-dependencies

COPY . ./
RUN cabal install

# Stage 2: Create the final image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ffmpeg libpq5 zlib1g

WORKDIR /usr/local/bin

# Copy the built executable from the builder stage
COPY --from=builder /root/.local/bin/mqttd .

VOLUME /etc/mqttd
WORKDIR /etc/mqttd

COPY mqttd.conf .
RUN sed -i 's/:memory:/mqttd.db/' /etc/mqttd/mqttd.conf

CMD ["/usr/local/bin/mqttd"]
