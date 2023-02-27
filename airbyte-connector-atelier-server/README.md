# Connector Atelier


## Getting started

Install dependencies, compile, and build the server
```bash
./gradlew :airbyte-connector-atelier-server:build
```

Then run the server (You can also do this w/o build)
```bash
./gradlew :airbyte-connector-atelier-server:run
```

The server is now reachable on localhost:80

## Changing the used CDK version

Update the `airbyte-connector-atelier-server/CDK_VERSION` file to point to the target version.
This will make sure both this project and the webapp depending on it will stay in sync.

## OpenAPI generation

Run it via Gradle by running this from the Airbyte project root:
```bash
./gradlew :airbyte-connector-atelier-server:generateOpenApiServer
```

## Migrating the module to `airbyte-connector-builder-server`

For ease of development while migrating to the Micronaut server, we will handle all development on this
separate module. However, what we really want moving forward is to replace the old module with this new
one. We're using the word `atelier` (def: a workshop or studio, especially one used by an artist or designer)
to delineate every instance that should be renamed from atelier to builder because the word is not used
anywhere else in the codebase. When it comes time to switch to the new server, we will remove the original
`airbyte-connector-builder-server` module and rename everywhere in the code and every folder where atelier is
mentioned.