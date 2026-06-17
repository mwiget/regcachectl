# Minimal image for the credential-free repo.f5.com blob cache.
# Built from a statically-linked binary (see `make blobcache-image`), so the
# image needs no Go toolchain and no shell — distroless/static includes the CA
# bundle required for TLS to repo.f5.com and the signed download URLs.
FROM gcr.io/distroless/static-debian12
COPY bin/regcachectl-linux /regcachectl
ENTRYPOINT ["/regcachectl"]
