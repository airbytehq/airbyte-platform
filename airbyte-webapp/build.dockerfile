FROM node:18.15.0-slim AS base

ENV PNPM_HOME=/pnpm
ENV PATH="$PNPM_HOME:$PATH"
ENV NPM_CONFIG_PREFIX=/pnpm
ARG PNPM_VERSION=8.6.12
ARG PNPM_STORE_DIR=/pnpm/store
ARG PROJECT_DIR

RUN apt update && apt install -y \
    curl \
    git \
    xxd

RUN corepack enable && corepack prepare pnpm@${PNPM_VERSION} --activate
RUN pnpm config set store-dir $PNPM_STORE_DIR

COPY . /workspace
WORKDIR ${PROJECT_DIR}

FROM base AS prod-deps
RUN --mount=type=cache,id=pnpm,target=/pnpm/store pnpm install --prod --frozen-lockfile

FROM base AS build
RUN --mount=type=cache,id=pnpm,target=/pnpm/store pnpm install --frozen-lockfile
RUN pnpm build

FROM base AS common
COPY --from=prod-deps $PROJECT_DIR/node_modules/ $PROJECT_DIR/node_modules
COPY --from=build $PROJECT_DIR $PROJECT_DIR/build/app
