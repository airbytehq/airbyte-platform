FROM node:18.15.0-slim

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

# Install Cypress dependencies
RUN apt install -y libgtk2.0-0 libgtk-3-0 libgbm-dev libnotify-dev libgconf-2-4 libnss3 libxss1 libasound2 libxtst6 xauth xvfb

RUN corepack enable && corepack prepare pnpm@${PNPM_VERSION} --activate
RUN pnpm config set store-dir $PNPM_STORE_DIR

COPY . /workspace
WORKDIR ${PROJECT_DIR}

RUN --mount=type=cache,id=pnpm,target=/pnpm/store pnpm install --prod --frozen-lockfile
RUN --mount=type=cache,id=pnpm,target=/pnpm/store pnpm install --frozen-lockfile
RUN pnpm generate-client
RUN pnpm cypress install
