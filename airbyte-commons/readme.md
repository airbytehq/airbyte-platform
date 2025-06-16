# airbyte-commons

Common java helpers.

This submodule is inherited by all other java modules in the monorepo! It is therefore important that we do not add dependencies to it, as those
dependencies will also be added to every java module. The only dependencies that this module uses are the ones declared in the `build.gradle` at the
root of the Airbyte monorepo. In other words it only uses dependencies that are already shared across all modules. The `dependencies` section of
the `build.gradle` of `airbyte-commons` should always be empty.

For other common java code that needs to be shared across modules that requires additional dependencies, we follow this
convention: `airbyte-commons-<name of lib>`. See for example `airbyte-commons-micronaut`.

# Updating the secrets mask

This module contains a secrets mask configuration file, which is baked into the JAR.
This is used by logging systems to mask secrets from log messages.

Run ./gradlew :downloadSpecSecretMask to manually update this file.