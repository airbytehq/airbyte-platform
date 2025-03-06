FROM bitnami/kubectl:1.31.4

# Create the Airbyte User
USER root
RUN groupadd --gid 1000 airbyte && useradd --uid 1000 -g airbyte --create-home --no-log-init airbyte

USER airbyte:airbyte

COPY --chown=airbyte:airbyte sweep-pod.sh /app/sweep-pod.sh
RUN chmod +x /app/sweep-pod.sh

ENTRYPOINT "/app/sweep-pod.sh"