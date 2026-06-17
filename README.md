# regcachectl

A local **fleet of `registry:2` pull-through caches** — one container per
upstream registry — so repeatedly created/destroyed **k3s-in-docker** clusters
(tmmlitectl, ocibnkctl, …) stop re-pulling the same images from the public and
private registries on every rebuild.

It is **tool-agnostic**: it programs only the local container runtime (docker or
podman) and emits a standard k3s `registries.yaml` snippet, so any ctl tool can
point its nodes at the same fleet. The caches use `--restart=always` + persistent
named volumes, so they survive **cluster teardown _and_ host reboots**.

## Why one container per upstream

containerd's registry mirror is keyed by the upstream host and forwards the
original repo path with **no host prefix**, so a single endpoint serving multiple
upstreams would be ambiguous (`docker.io/library/redis` vs `quay.io/x/redis`
collide). One pull-through cache per upstream maps 1:1 to k3s mirror semantics.

| Cache | Upstream | Proxies to | Host port |
|---|---|---|---|
| `regcache-dockerhub` | `docker.io` | `https://registry-1.docker.io` | 5000 |
| `regcache-ghcr` | `ghcr.io` | `https://ghcr.io` | 5001 |
| `regcache-quay` | `quay.io` | `https://quay.io` | 5002 |
| `regcache-f5` | `repo.f5.com` | `https://repo.f5.com` | 5003 |

## Authentication

The private `repo.f5.com` (GCP Artifact Registry) upstream needs credentials.
Pass the operator's FAR tgz with `--far-key`; `regcachectl` extracts the
`_json_key_base64` / service-account credential and bakes it into the F5 cache as
`REGISTRY_PROXY_USERNAME/PASSWORD`. **The F5 credential then lives in exactly one
place** — clients pull anonymously through the cache. Without `--far-key`, the F5
cache is skipped and the three public caches still come up.

## Usage

```bash
make install                                   # → ~/.local/bin/regcachectl

regcachectl up --far-key keys/f5-far-auth-key.tgz   # create/start the fleet (idempotent)
regcachectl status                                  # state, disk use, reachability
regcachectl print-registries > registries.yaml      # the k3s wiring snippet
regcachectl gc                                       # reclaim space (garbage-collect)
regcachectl down                                     # stop & remove (keeps cached blobs)
regcachectl down --purge                             # also drop the cached blobs
```

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
sudo regcachectl install-systemd --far-key /abs/path/keys/f5-far-auth-key.tgz --write
sudo systemctl enable --now tmm-regcache.service
```

## Live-verified

- public + **F5 GAR token-dance** pull-through both return real manifests/blobs;
- a full `docker pull` through the cache populates layers (cache disk grows),
  and a second pull is served locally;
- `up` is idempotent; volumes + `restart=always` persist across teardown/reboot.

## Build

```bash
make build     # → bin/regcachectl
make test      # unit tests (far extraction, registries render)
make smoke     # tests + read-only CLI assertions
```

Zero external Go dependencies (stdlib only).
