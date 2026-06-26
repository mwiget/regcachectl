# regcachectl

[![regcachectl](https://img.shields.io/badge/image%20cache-regcachectl-2496ed?logo=docker&logoColor=white)](https://github.com/mwiget/regcachectl)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)
![Last commit](https://img.shields.io/github/last-commit/mwiget/regcachectl)

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
| `regcache-nvcr` | `nvcr.io` | `registry:2` anonymous proxy (relays the client's NGC token) | 5004 |

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
regcachectl list                                # cached objects + space per cache
regcachectl list --objects                      # image-level inventory (repo:tag) for every cache
regcachectl list --blobs                        # F5 cache: per-layer digests + sizes + the images they belong to
regcachectl pull nvcr.io/nvidia/doca/dpf-system:v26.4.0   # warm a cache with EVERY platform of an image
regcachectl export -o regcache.tgz              # bundle every cache into one .tgz to copy to another host
regcachectl export -o nvcr.tgz --cache nvcr     # bundle only one cache (e.g. just the warmed nvcr image)
regcachectl import regcache.tgz                 # unpack a bundle into this host's cache volumes (offline seed)
regcachectl print-registries > registries.yaml  # the k3s wiring snippet
regcachectl gc                                   # reclaim space in the public caches
regcachectl down                                 # stop & remove (keeps cached blobs)
regcachectl down --purge                         # also drop the cached blobs
```

`list` reports what each cache actually holds and how much space it uses:

```
docker.io    [registry:2 :5000]  running — 9 images, 168.3M  (shared blob store; size is the cache total)
repo.f5.com  [blobcache :5003]   running — 4 blobs, 117.1MB

blob-cache total: 117.1MB
```

The F5 blob cache is digest-keyed (it stores *layers*, not images), but
`--objects` presents it at the **image level** like the `registry:2` caches —
one `repo:tag` line per image:

```
repo.f5.com  [blobcache :5003]  running — 15 images (127 blobs), 560.3MB  (shared layers, size is the cache total)
    images/tmm-img:v2.3.0
    images/f5-dssm-store:v2.3.0
    images/rabbit:v2.3.0
    …
```

`--blobs` drops to the layer detail — each digest, its exact size, and the
image(s) it belongs to (a shared base layer lists them all):

```
repo.f5.com  [blobcache :5003]  running — 127 blobs, 560.3MB  (shared layers, size is the cache total)
    sha256:b2a7a667…aecb79f  58.8MB  images/f5-dssm-store, images/ocnos-img-init, images/rabbit
    sha256:e6f7c758…1a1709    58.4MB  images/tmm-img
```

### How the names + tags are recovered

The blob cache never caches manifests (that's what keeps it credential-free), so
it has no built-in blob→image map. It reconstructs one from the request paths,
neither of which is a credential:

- **repo** comes from each blob path (`/v2/<repo>/blobs/<digest>`), recorded per
  digest;
- **tag** comes from each manifest path (`/v2/<repo>/manifests/<ref>`) — relayed,
  never cached, only noted. A digest-pinned image is requested by digest (no tag
  is ever sent), so it lists as `repo@sha256:short` instead of `repo:tag` — it
  reads as "pinned by digest", not "missing a tag". The `registry:2` caches do
  the same: a digest-pulled image (e.g. Cilium, whose helm chart pins by digest)
  shows `repo@sha256:short` — the image-index digest read from its on-disk
  manifest revisions, with the per-platform child manifests collapsed away.

Both are captured on **every** request — including a cache HIT — so the names and
tags fill in the next time any pull touches the image, even a fully warm redeploy
that fetches nothing upstream. A layer cached before this (or not yet
re-requested) lists as `(N unnamed layer(s) — re-pull to record)`.

For the public `registry:2` caches `--objects` lists the **truly-cached**
repo:tags (read from the on-disk manifest store — the registry tags API proxies
upstream and would over-report), and a shared-store total (per-repo sizes aren't
attributable because blobs are shared).

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

### Moving the cache to another host

`export` bundles every cache's on-disk data into one `.tgz`; `import` unpacks it
into another host's cache volumes — so you can warm a second machine **offline**,
without re-pulling gigabytes from the upstreams:

```bash
regcachectl export -o regcache.tgz      # on the warm host  → one .tgz
scp regcache.tgz other-host:            # copy it across
# on the other host:
regcachectl import regcache.tgz         # unpack into the cache volumes
regcachectl up                          # serve it
```

Both stream through a throwaway helper container (the already-present registry
image, which has `tar`) that mounts the volumes — `export` read-only, so the
fleet keeps serving while you bundle. Cache data is content-addressed and
immutable, so `import` is a safe **union**: it seeds a fresh fleet or merges into
an existing one without clobbering blobs. The image names + tags ride along (the
sidecar index is part of each volume), so the imported `list --objects` reads
identically on the new host.

### Seeding a multi-arch image across an air gap (`pull`)

`export`/`import` only carries what the cache already holds — and a `docker pull`
+ `docker save` keeps only the host platform (an arm64 Mac strips the amd64 image
an x86 server needs, and `ctr import` then fails with "content digest … not
found"). `regcachectl pull` fixes that by warming the cache with **every**
platform: it walks the manifest index and fetches each child's config + layers
through the cache, so the bundle is genuinely multi-arch.

```bash
# on a host that CAN reach nvcr.io (e.g. a Mac with an NGC key in ~/.ngc):
regcachectl up
regcachectl pull nvcr.io/nvidia/doca/dpf-system:v26.4.0   # all platforms → regcache-nvcr (:5004)
regcachectl export -o regcache.tgz
scp regcache.tgz blocked-host:

# on the WAF-blocked host:
regcachectl import regcache.tgz && regcachectl up         # serves nvcr.io HITs, no upstream contact
regcachectl print-registries                              # wire k3s: mirrors "nvcr.io" → :5004
```

The fleet holds **no** credentials: `pull` mints the NGC bearer token from your
`~/.ngc` (or `--creds '$oauthtoken:<key>'`) against nvcr.io's auth realm and
replays it through the cache. Public multi-arch images warm anonymously
(`regcachectl pull docker.io/library/redis:7`). Imported HITs serve without
re-checking upstream auth, which is exactly what an air-gapped/blocked host needs.

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
