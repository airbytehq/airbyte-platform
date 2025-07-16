# Airbyte Keycloak theme

This directory contain a [keycloak themes](https://www.keycloak.org/docs/latest/server_development/#_themes) for Airbyte Cloud and Self Managed Enterprise. 

The `airbyte-keycloak-theme` is based on the built-in Keycloak `common/base` theme, which provides minimal HTML templates and internationalized strings that the Airbyte theme builds upon. The `airbyte-cloud` theme extends `airbyte-keycloak-theme` with some cloud-specific styling.

## Developing the theme

The development process is currently not very streamlined, and there is no development mode or frontend build process to speak of. The template files in this directory can be edited individually, then the docker image must be rebuilt and the cluster can be started to view/test the changes made.

First, the entryoint script for Keycloak must be altered to disable caching. The `entrypoint.sh` script located in `airbyte-keycloak/scripts` has a commented startup script that should be enabled:

```sh
# Uncomment to disable caching, which is useful for theme development
# bin/kc.sh start --optimized --proxy edge --hostname-strict false --spi-theme-static-max-age=-1 --spi-theme-cache-themes=false --spi-theme-cache-templates=false
```

After this, the `airbyte-keycloak` image needs to be built with gradle:

`./gradlew :oss:airbyte-keycloak:assemble`.

Then you can use the `make deploy.cloud` command to deploy Airbyte Cloud locally, or use helm directly to redeploy `airbyte-keycloak` for a Self Managed Enterprise instance. 

## Making CSS changes

It's easiest to make CSS changes directly in the browser. Once you are happy with your changes, you can copy them all (e.g. from Chrome's `inspector-stylesheet.css`) into the appropriate CSS file in the Keycloak theme. Then `airbyte-keycloak` can be rebuilt and redeployed (`make deploy`) for your changes to be baked into the docker image.

## Overriding templates

The HTML content of Keycloak pages is defined in FreeMarker templates with the `.ftl` extension. If you want to alter the HTML content of a page, you should copy the contents of the template you want to override from Keycloak's GitHub repository:

```
https://github.com/keycloak/keycloak/blob/<keycloak-version>/themes/src/main/resources/theme/base/login/<template-to-override>.ftl
```

*Important*: make sure you copy the content from the correct `<keycloak-version>` that matches the Keycloak version we are currently using, which may not necessarily be the most recent release.

## Registering the theme in Keycloak

The theme for a realm can be changed in the Keycloak UI under `Realm settings` > `Themes`. Alternatively it can be set in the terraform provider or via the Airbyte Java SDK.
