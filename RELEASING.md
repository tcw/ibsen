# Releasing

Tag, push, done:

```shell
git checkout master && git pull
git tag v0.4.0
git push origin v0.4.0
```

The tag runs [`.github/workflows/release.yml`](.github/workflows/release.yml), which runs
GoReleaser with [`.goreleaser.yaml`](.goreleaser.yaml). It publishes:

- **Binaries** for Linux, macOS and Windows on amd64 and arm64, and Linux on 32-bit ARM,
  built with `CGO_ENABLED=0`, `-trimpath` and `-ldflags="-s -w"`, with the version stamped in
  so `ibsen --version` prints it.
- **Archives** per platform (`.tar.gz`, `.zip` on Windows) holding the binary, the README and
  the guides, plus `checksums.txt`.
- **Release notes** made from the commits since the previous tag.
- **Container images** for linux/amd64 and linux/arm64 at `ghcr.io/tcw/ibsen`, tagged with the
  version and `latest`.

Nothing else has to happen first. The generated gRPC code is committed, so no `protoc` runs
at release time, and CI has already run the tests on the commit you tag.

## Before you tag

- CI is green on the commit.
- The `release-snapshot` job in CI builds the whole release on every push to master without
  publishing it, and runs [`scripts/smoke-test.sh`](scripts/smoke-test.sh) — `--version` and
  the README's quick start — against the Linux binary. If it is green, the release will build.

To build the same snapshot locally, with GoReleaser installed:

```shell
goreleaser release --snapshot --clean
./scripts/smoke-test.sh dist/ibsen_linux_amd64_v1/ibsen
```

## Versions

Tags are semantic versions with a leading `v`. A binary built without a release stamp reports
the module version `go install` resolved, or `dev` for a build from a working tree.

## Not yet

A Homebrew tap and signed artifacts.
