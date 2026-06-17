# regcachectl

A local **fleet of `registry:2` pull-through caches** — one container per
upstream registry — so repeatedly created/destroyed **k3s-in-docker** clusters
(tmmlitectl, ocibnkctl, …) stop re-pulling the same images from the public and
private registries on every rebuild.

It is **tool-agnostic**: it programs only the local container runtime (docker or
podman) and emits a standard k3s `registries.yaml` snippet, so any ctl tool can
point its nodes at the same fleet. The caches use `--restart=always` + persistent
named volumes, so they survive **cluster teardown _and_ host reboots**.

**The fleet stores no registry credentials.** Public registries are cached
anonymously. The private `repo.f5.com` is a **credential-free blob cache**: each
client (k3s cluster) supplies its own FAR key via its `registries.yaml`, so
different BNK versions (GA vs engineering builds, which need different keys) can
pull through the same shared cache.

## Why one container per upstream

containerd's registry mirror is keyed by the upstream host and forwards the
original repo path with **no host prefix**, so a single endpoint serving multiple
upstreams would be ambiguous (`docker.io/library/redis` vs `quay.io/x/redis`
collide). One pull-through cache per upstream maps 1:1 to k3s mirror semantics.

| Cache | Upstream | Engine | Host port |
|---|---|---|---|
| `regcache-dockerhub` | `docker.io` | `registry:2` anonymous proxy | 5000 |
| `regcache-ghcr` | `ghcr.io` | `registry:2` anonymous proxy | 5001 |
| `regcache-quay` | `quay.io` | `registry:2` anonymous proxy | 5002 |
| `regcache-f5` | `repo.f5.com` | credential-free blob cache | 5003 |

## Authentication — the client supplies the key, not the cache

`repo.f5.com` is a GCP Artifact Registry: it needs a credential, and it serves
large layers via **302 redirects** to pre-signed download URLs. `registry:2`
proxy mode would cache those layers but only with a *static* key baked into the
cache. Instead the F5 cache is a small **credential-relaying, redirect-following,
digest-keyed proxy** (`serve-blobcache`):

- it **relays the client's `Authorization`** upstream for manifests/tokens
  (never caching them, so auth is always enforced upstream);
- for blob GETs it serves a disk **HIT by sha256 digest**, or on a MISS forwards
  the client's auth, **follows GAR's 302** to the signed URL (no auth — it's
  pre-signed), and streams + caches the verified blob.

So the cache holds **no credential**. The client (each k3s cluster) presents its
own FAR key via the `configs:` block of its `registries.yaml` — which is exactly
what tmmlitectl renders per-PoC from that PoC's `keys/`. (A cached blob is served
on a HIT without re-checking upstream auth — same trust posture as a `registry:2`
pull-through cache — so bind the fleet to a trusted local host.)

## Usage

```bash
make install                                   # builds the binary + blob-cache image,
                                               # installs → ~/.local/bin/regcachectl

regcachectl up                                  # create/start the fleet (idempotent, no creds)
regcachectl status                              # state, disk use, reachability
regcachectl print-registries > registries.yaml  # the k3s wiring snippet
regcachectl gc                                   # reclaim space in the public caches
regcachectl down                                 # stop & remove (keeps cached blobs)
regcachectl down --purge                         # also drop the cached blobs
```

> The F5 blob-cache image is built by `make blobcache-image` (run automatically
> by `make install`). If it's missing, `up` brings the public caches up and skips
> the F5 cache with a note.

### Wiring a k3s cluster to the fleet

`print-registries` emits a `mirrors:` block that lists the cache first and the
real upstream second as a **fallback** (a stopped cache degrades to direct pulls
instead of breaking deploys):

```yaml
mirrors:
  "repo.f5.com":
    endpoint:
      - "http://host.docker.internal:5003"
      - "https://repo.f5.com"
```

Mount it into each k3s node at `/etc/rancher/k3s/registries.yaml` and give the
node containers `--add-host host.docker.internal:host-gateway` so they can reach
the host-published caches. (In tmmlitectl this is the opt-in `cluster.registry_cache`
poc.yaml knob — see that repo. For manual use, pass `--host <bridge-gateway-ip>`.)

### Surviving reboots

The caches carry `--restart=always`, so the docker daemon restarts them on boot.
For a belt-and-suspenders reconcile-on-boot unit:

```bash
sudo regcachectl install-systemd --write
sudo systemctl enable --now tmm-regcache.service
```

## Live-verified

- a full `docker pull` through the public caches populates layers and a second
  pull is served locally;
- a real `repo.f5.com/images/tmm-img` pull through the **credential-free** F5
  blob cache — with the FAR key supplied only by the client (k3s
  `registries.yaml` `configs`) — caches all layers (cache 4K → 117 MB); a re-pull
  is an all-HIT served from cache with no upstream download (10.5 s → 3.7 s);
- `up` is idempotent; volumes + `restart=always` persist across teardown/reboot.

## Build

```bash
make build            # → bin/regcachectl
make blobcache-image  # → regcache-blobcache:latest (the F5 blob cache)
make test             # unit tests (blobcache proxy, registries render)
make smoke            # tests + read-only CLI assertions
```

Zero external Go dependencies (stdlib only).
