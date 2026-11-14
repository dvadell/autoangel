# The Engine release image. Build it natively on each architecture:
#
#   podman build -f docker/engine.Dockerfile -t autoangel-engine:<rev>-<arch> .
#
# No kernel is promoted to Rust (docs/decisions.md), so the release holds no NIF;
# it is the same code on linux/amd64 and linux/arm64. The plugin set is fixed in
# engine/config/prod.exs at build time.
ARG ELIXIR_IMAGE=docker.io/library/elixir:1.19.5-otp-27-slim
ARG RUNTIME_IMAGE=docker.io/library/debian:bookworm-slim

FROM ${ELIXIR_IMAGE} AS build
ENV MIX_ENV=prod LANG=C.UTF-8
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git ca-certificates \
    && apt-get clean
WORKDIR /src
RUN mix local.hex --force && mix local.rebar --force
COPY shared ./shared
COPY engine/mix.exs engine/mix.lock ./engine/
COPY engine/config ./engine/config
COPY engine/apps ./engine/apps
WORKDIR /src/engine
RUN mix deps.get --only prod && mix release autoangel --overwrite

FROM ${RUNTIME_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 libstdc++6 libncurses6 ca-certificates curl \
    && apt-get clean \
    && useradd --system --uid 10001 --home /var/lib/autoangel autoangel \
    && mkdir -p /var/lib/autoangel/state /etc/autoangel \
    && chown -R autoangel /var/lib/autoangel
COPY --from=build --chown=autoangel /src/engine/_build/prod/rel/autoangel /opt/autoangel
USER autoangel
ENV AUTOANGEL_CONFIG=/etc/autoangel/engine.toml \
    AUTOANGEL_STATE_PATH=/var/lib/autoangel/state \
    LANG=C.UTF-8
VOLUME ["/var/lib/autoangel/state"]
# The ports are set in the configuration file ([http]); these are the defaults.
EXPOSE 9400 9401
ENTRYPOINT ["/opt/autoangel/bin/autoangel"]
CMD ["start"]
