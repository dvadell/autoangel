# Architecture decisions

## Storage gate

- CubDB checkpoint (20000 Series, 4500 byte state): write 2249.613 ms, restore 349.488 ms, replacement 7277.94 ms, on-disk size 600578091 bytes (restored 20000 entries).

The M0 storage benchmark results and the decision on checkpoint and TimescaleDB write paths will be recorded here.

## NIF gate

The M0 NIF benchmark results will determine which numeric kernels, if any, move from the Elixir reference implementation to Rust NIFs. The selected implementation and measured limits will be recorded here.

## Provisional defaults

Defaults marked provisional in the design remain subject to the M0 benchmark. Changes and their measured rationale will be recorded here.
