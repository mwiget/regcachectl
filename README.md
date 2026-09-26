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
regcachectl print-registries --format crio --host 10.240.0.2 > registries.conf   # CRI-O / podman
regcachectl print-registries --format okd --host 10.240.0.2 > regcache.yaml      # OKD IDMS/ITMS/Image
regcachectl pull-release quay.io/okd/scos-release@sha256:…  # warm an OKD release + its whole payload
regcachectl list --okd-release quay.io/okd/scos-release@sha256:…  # "N/M payload images cached"
regcachectl gc                                   # reclaim orphaned blobs (keeps digest-pulled images)
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

### Wiring CRI-O and OKD nodes

`--format crio` emits a `containers-registries.conf(5)` with one `[[registry]]`
per cache and the cache as an insecure (plain-HTTP) `[[registry.mirror]]`; CRI-O
falls back to the upstream itself when the cache is down, so `--no-fallback` is
refused for this format. `--format okd` emits an `ImageDigestMirrorSet`, an
`ImageTagMirrorSet` and the `cluster` `Image` with the caches in
`spec.registrySources.insecureRegistries`; `--no-fallback` there sets
`mirrorSourcePolicy: NeverContactSource`. Both use `--host` (the address the
nodes reach the host at) and the same port-by-index layout as the k3s output.
Applying the `Image` replaces its spec — merge it into an existing one on a
running cluster.

### Warming an OKD release (`pull-release`)

```bash
regcachectl pull-release -j 8 quay.io/okd/scos-release@sha256:65f272bc…   # 4.22.0-okd-scos.10
regcachectl list --okd-release quay.io/okd/scos-release@sha256:65f272bc…
regcachectl export --cache quay -o okd-4.22.tgz                           # an offline OKD
```

`pull-release` reads `release-manifests/image-references` from the release
image (top layer first), then pulls every payload image it names through its
cache, `-j` at a time, with each shared layer fetched once. `--platform`
(default `linux/amd64`) picks the child of multi-arch images; `--platform ''`
warms all. A cold cache serving one uncached blob to several nodes at once is
where installs failed (`unexpected EOF (after reconnecting, server did not
process a Range: header)`); warmed first, every node pull is a hit. For
4.22.0-okd-scos.10 (191 payload images, 458 distinct blobs) a warm of an empty
cache from quay.io with `-j 8` took 2m7s and stored 17.2 GB; re-running it
against the warm cache with the upstream unreachable took 2 s.

`pull-release` then checks what the cache actually stored, the same on-disk check
`list --okd-release` makes, and re-warms what is missing (up to two rounds); its
final count is that verified count. A `200` is not enough: while another client's
fetch of the same blob is in flight, the proxy streams it straight from the
upstream without storing it, and if that other client hangs up the blob is
stored by nobody. Against a throwaway registry:3, with three slow clients hanging
up mid-fetch, a pass that reported 191/191 left 3 images incomplete on disk; the
re-warm made them 191/191 verified. Flags may go before or after the release
ref (`pull-release <ref> -j 8`).

`list --okd-release` answers from the cache's on-disk store without fetching
anything; add `--objects` to list the payload images still missing.

### Cache lifetime and `gc`

The registry caches run `registry:3.1.2` (distribution v3). `registry:2.8`'s
proxy deletes every blob and manifest a fixed 7 days after it first fetched it,
whether or not it was used since — a warmed OKD payload or an imported air-gap
bundle expires a week later. v3 makes that `proxy.ttl` configurable; `up
--proxy-ttl` sets it and defaults to `0`, keep until `gc`/`down --purge`. Tags
are still re-resolved upstream on every pull, so a moved tag is followed.
v3 reads the same on-disk layout: an existing 2.8.3 volume serves unchanged, and
a v3-written volume serves under 2.8.3 again. `up` never recreates a running
cache; it says when one runs another image, and `regcachectl down && regcachectl
up` switches it with the cached data kept.

`gc` stops each registry cache, runs `registry garbage-collect` against its
volume in a throwaway container of the same image, and starts it again. Running
it against the serving registry is unsafe: the running process keeps a stale
in-memory record of every blob it deletes and answers `200` with the full
`Content-Length` and an empty body until restarted. By default it only removes
blobs no cached manifest references. `gc --delete-untagged` also deletes every
manifest no tag points at — in a pull-through cache that is everything pulled by
digest: on a cache holding OKD 4.22 it would delete 658 of 707 blobs. (`registry:2.8`
refuses `--delete-untagged` outright when a repository holds no tag.)

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
