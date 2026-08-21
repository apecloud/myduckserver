# Release and Image Policy

MyDuck Server images are published to `docker.io/apecloud/myduckserver`.
Application source, release tooling, and acceptance are separate steps so a
published image can always be traced and rolled back.

## Tag policy

| Tag | Example | Mutable | Created by |
| --- | --- | --- | --- |
| Development version | `v0.1.0-dev.20260821.1` | No | Release Image workflow |
| Release candidate | `v0.1.0-rc.1` | No | Release Image workflow |
| Source commit | `sha-5e29d94d` | No | Release Image workflow |
| Stable version | `v0.1.0` | No | Promote Image workflow after acceptance |
| Stable channel | `latest` | Yes | Promote Image workflow after acceptance |

Development and release-candidate builds use semantic versions with the
formats shown above. Bug fixes increment the patch version. Backward-compatible
features increment the minor version. Breaking changes increment the major
version.

The workflows refuse to overwrite a version or source-commit tag. A stable
version tag may be used again only when it already points to the exact accepted
digest. A push to `main` never publishes or moves `latest`.

## Prerequisites

Repository Actions must be enabled. The existing Docker Hub credentials must
be available as `DOCKER_REGISTRY_USER` and `DOCKER_REGISTRY_PASSWORD` Actions
secrets.

## Build an immutable prerelease

Run the **Release Image** workflow from the `main` branch with:

- `source_sha`: the full, lowercase, 40-character application commit SHA;
- `version`: a development or release-candidate version.

The workflow checks out that exact SHA into an isolated directory, compares the
actual checkout SHA byte for byte with the input, and verifies that it is an
ancestor of the `main` commit from which the workflow was dispatched. The
workflow definition may be newer than the application source, but the build
context and Dockerfile both come from the requested application commit.

One multi-architecture build publishes the version and commit tags. Both tags
must resolve to the digest returned by the build. The workflow uploads
`release-metadata.json` with the source and workflow SHAs, run ID and URL,
immutable tags, top-level digest, per-platform digests, and build time.

The first restored baseline uses:

```text
source_sha: 5e29d94db535e51876ec9465c5ef78a8e2c2d92a
version: v0.1.0-dev.20260821.1
commit tag: sha-5e29d94d
```

This baseline predates the `myduckserver --version` command. Its version and
source commit must be queried from its OCI labels. Do not apply the release
tooling commit to the baseline source tree.

## Inspect build identity

All images contain standard OCI labels, including source repository, version,
full source revision, and build time:

```bash
docker pull apecloud/myduckserver@sha256:<digest>
docker image inspect apecloud/myduckserver@sha256:<digest> \
  --format '{{json .Config.Labels}}'
```

Images built from this release tooling commit or later also expose the same
identity from the binary:

```bash
docker run --rm --entrypoint myduckserver \
  apecloud/myduckserver@sha256:<digest> --version
```

## Accept and promote without rebuilding

Independent acceptance must cover startup, MySQL and PostgreSQL queries,
initialization scripts, persistent data, and restart recovery. Record the test
report or task reference before promotion.

Only an accepted release candidate may become stable. Run the **Promote
Image** workflow from the `main` branch with:

- the full source SHA;
- the existing release-candidate version;
- the accepted `sha256:...` digest;
- the new stable version;
- the independent acceptance reference.

The workflow verifies that the source commit is reachable from `main` and that
both the prerelease tag and `sha-<commit>` tag point to the accepted digest. It
then adds the stable version and `latest` to that digest with
`docker buildx imagetools create`. It does not invoke a build. The workflow
inspects both promoted tags afterward and uploads `promotion-metadata.json`,
including the workflow run, per-platform digests, acceptance reference, and
previous `latest` digest.

For example, after accepting `v0.1.0-rc.1`, promote its existing digest to
`v0.1.0` and `latest`. The immutable image still reports its original build
version (`v0.1.0-rc.1`); the stable tag records that this exact candidate passed
release acceptance.

## Roll back `latest`

Do not rebuild an old version and do not move its immutable tags. Re-run the
**Promote Image** workflow using the previous stable version, its original
prerelease tag, source SHA, digest, and acceptance reference. The workflow
verifies all immutable references and moves only `latest` back to that digest.

Every release record must keep:

- workflow run URL;
- full application source SHA;
- prerelease, source-commit, and stable tags as applicable;
- multi-architecture digest;
- acceptance result and reference;
- build or promotion time;
- previous `latest` digest for rollback.
