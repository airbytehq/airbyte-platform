ARG NODE_VERSION
FROM node:${NODE_VERSION}-slim

ENV PNPM_HOME=/pnpm
ENV PATH="$PNPM_HOME:$PATH"
ENV NPM_CONFIG_PREFIX=/pnpm
ARG PNPM_STORE_DIR=/pnpm/store
ARG PROJECT_DIR

RUN apt update && apt install -y \
    curl \
    git \
    xxd \
    jq

# Install Cypress dependencies
RUN apt install -y libgtk2.0-0 libgtk-3-0 libgbm-dev libnotify-dev libgconf-2-4 libnss3 libxss1 libasound2 libxtst6 xauth xvfb

COPY . /workspace
WORKDIR ${PROJECT_DIR}

RUN corepack enable && corepack install
RUN pnpm config set store-dir $PNPM_STORE_DIR

RUN --mount=type=cache,id=pnpm,target=/pnpm/store pnpm install --frozen-lockfile
RUN pnpm generate-client
RUN pnpm cypress install
