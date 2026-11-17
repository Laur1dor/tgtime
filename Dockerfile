FROM debian:bookworm-slim
COPY --chmod=0755 tgtime /usr/local/bin/tgtime
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/tgtime"]
