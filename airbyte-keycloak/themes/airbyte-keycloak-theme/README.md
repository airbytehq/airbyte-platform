# Airbyte Keycloak theme

This directory contains a [keycloak theme](https://www.keycloak.org/docs/latest/server_development/#_themes) for Airbyte. The theme is based on the `common/base` theme, which provides minimal HTML templates and internationalized strings that the Airbyte theme builds upon.

## Developing the theme

The development process is currently not very streamlined, and there is no development mode or frontend build process to speak of. The template files in this directory can be edited individually, then the docker image must be rebuilt and the cluster can be started to view/test the changes made.

It's recommended to run Keycloak with caching disabled. The `entrypoint.sh` script located in `airbyte-keycloak/scripts` should be altered to pass several flags:

```sh
bin/kc.sh start
    --optimized
    --http-port "$KEYCLOAK_PORT"
    --proxy edge
    --hostname-strict false
    --spi-theme-static-max-age=-1
    --spi-theme-cache-themes=false
    --spi-theme-cache-templates=false
```

After this, the `airbyte-keycloak` image needs to be built with gradle:

`./gradlew -p oss :airbyte-keycloak:assemble`.

Then the kubernetes cluster can be started with `helm`. The `--set keycloak.image.tag=dev` ensures that helm uses the image tagged `dev` that you built in the previous step.

```
cd oss/charts/airbyte
helm install ab . --set keycloak.image.tag=dev
```

Once the helm deploy succeeds, you can set up a port forward to the webapp:

```
k port-forward svc/ab-airbyte-webapp-svc 8000:80
```

Note: the port forward will fail if the webapp service is not yet running. You will likely have to wait a few seconds for the service to be ready.

## Registering the theme in Keycloak

The theme is automatically registered as part of the startup process in `airbyte-keycloak-setup`. The theme for a realm can also be changed in the Keycloak UI under `Realm settings` > `Themes`.
