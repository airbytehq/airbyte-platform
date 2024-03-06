ARG NODE_VERSION
FROM node:${NODE_VERSION}-slim AS base

ENV PNPM_HOME=/pnpm
ENV PATH="$PNPM_HOME:$PATH"
ENV NPM_CONFIG_PREFIX=/pnpm
ARG PNPM_STORE_DIR=/pnpm/store
ARG PROJECT_DIR

RUN apt update && apt install -y \
    curl \
    git \
    xxd

COPY . /workspace
WORKDIR ${PROJECT_DIR}

RUN corepack enable && corepack install
RUN pnpm config set store-dir $PNPM_STORE_DIR

FROM base AS build
RUN --mount=type=cache,id=pnpm,target=/pnpm/store pnpm install --frozen-lockfile
RUN pnpm build

FROM base AS common
COPY --from=build $PROJECT_DIR $PROJECT_DIR/build/app
