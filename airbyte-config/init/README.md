# airbyte-config:init

This module fulfills two responsibilities:
1. It is where we declare icons for loading connector definitions. This will soon be deprecated and moved entirely to the metadata service.
2. It contains the scripts and Dockerfile that allow the `docker-compose` version of Airbyte to mount the local filesystem. This is helpful in cases where a user wants to use a connector that interacts with (reads data from or writes data to) the local filesystem. e.g. `destination-local-json`.
