# Engine release

The release is defined in `engine/mix.exs` (`releases/0`). Its plugin set is fixed in
`engine/config/prod.exs`; its settings file is named by `AUTOANGEL_CONFIG`
(`engine/config/runtime.exs`). Build it with:

    cd engine && MIX_ENV=prod mix release autoangel

The image is built by `docker/engine.Dockerfile`; `scripts/smoke_release.sh` starts a
release to readiness.
