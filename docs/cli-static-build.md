# Static CLI builds

The CLI is released as a self-contained musl executable for:

- `x86_64-unknown-linux-musl`, linked with `x86_64-linux-musl-gcc`
- `aarch64-unknown-linux-musl`, linked with `aarch64-linux-musl-gcc`

The pinned Rust toolchain and locked dependency graph make releases repeatable;
install both Rust target standard libraries and the two version-pinned musl
cross-linkers in the build image. `.cargo/config.toml` selects the
architecture-specific musl linker and enables static CRT linking. The CLI's
HTTP client uses rustls, so TLS has no system-library dependency.

Install the two musl cross-linkers in the build image, then run:

```sh
./scripts/build_cli_static.sh
```

The script uses `--locked` for dependency reproducibility and verifies each
artifact with `file` and ELF program-header checks. It fails if either target
is missing, the artifact has the wrong architecture, or it contains an
interpreter or dynamic dependency. It prints a SHA-256 checksum for each
artifact.
