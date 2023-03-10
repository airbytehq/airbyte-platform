# Connector Atelier (aka Builder)

The temporary home for the Micronaut microservice version of the Connector Builder Server which will
be integrated back into `airbyte-connector-builder-server` upon completion.

## Getting started

Install dependencies, compile, and build the server
```bash
./gradlew -p oss airbyte-connector-atelier-server:build
```

Then run the server (You can also do this w/o build)
```bash
./gradlew -p oss airbyte-connector-atelier-server:run
```

The server is now reachable on localhost:80

## Running the new Micronaut server within Airbyte OSS

In addition to running the standalone server, you can also configure a local instance of Airbyte OSS to
use the version of the server built with Micronaut.

Replace the `airbyte-connector-builder-server` image name in `oss/docker-compose.yaml`:
```bash
airbyte-connector-builder-server:
  image: airbyte/connector-atelier-server:${VERSION}
```

Start up your local Airbyte instance:
```bash
VERSION=dev docker-compose up   
```

## OpenAPI generation

Run it via Gradle by running this from the Airbyte project root:
```bash
./gradlew -p oss airbyte-connector-atelier-server:generateOpenApiServer
```

## Migrating the module to `airbyte-connector-builder-server`

For ease of development while migrating to the Micronaut server, we will handle all development on this
separate module. However, what we really want moving forward is to replace the old module with this new
one. We're using the word `atelier` (def: a workshop or studio, especially one used by an artist or designer)
to delineate every instance that should be renamed from atelier to builder because the word is not used
anywhere else in the codebase. When it comes time to switch to the new server, we will remove the original
`airbyte-connector-builder-server` module and rename everywhere in the code and every folder where atelier is
mentioned.

## Changing the used CDK version

TODO: The current `connector-builder-server` and `airbyte-webapp` must stay in sync using the same version of
the `airbyte-cdk` package. We can specify this as an environment variable in the top-level `.env` file. 
This has not been implemented yet, but we may also need to implement this in a follow-up change.
