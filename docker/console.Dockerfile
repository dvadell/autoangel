# The Console release image (a separate release, with no Rust and no NIF):
#
#   podman build -f docker/console.Dockerfile -t autoangel-console:<rev>-<arch> .
#
# It also holds the `autoangel-console` escript for `mcp-stdio`.
ARG ELIXIR_IMAGE=docker.io/library/elixir:1.19.5-otp-27-slim
ARG RUNTIME_IMAGE=docker.io/library/debian:bookworm-slim

FROM ${ELIXIR_IMAGE} AS build
ENV MIX_ENV=prod LANG=C.UTF-8
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git ca-certificates \
    && apt-get clean
WORKDIR /src
RUN mix local.hex --force && mix local.rebar --force
COPY shared ./shared
COPY console/mix.exs console/mix.lock ./console/
COPY console/config ./console/config
COPY console/lib ./console/lib
WORKDIR /src/console
RUN mix deps.get --only prod && mix release autoangel_console --overwrite && mix escript.build

FROM ${RUNTIME_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 libstdc++6 libncurses6 ca-certificates curl \
    && apt-get clean \
    && useradd --system --uid 10002 --home /nonexistent autoangel
COPY --from=build --chown=autoangel /src/console/_build/prod/rel/autoangel_console /opt/autoangel_console
COPY --from=build /src/console/autoangel-console /usr/local/bin/autoangel-console
USER autoangel
ENV LANG=C.UTF-8
# AUTOANGEL_CONSOLE_PORT defaults to 4000 and the Console binds 127.0.0.1 unless
# AUTOANGEL_CONSOLE_BIND says otherwise.
EXPOSE 4000
ENTRYPOINT ["/opt/autoangel_console/bin/autoangel_console"]
CMD ["start"]
