# alpine-slim

> [!NOTE]
> If you publish a new version of this image, you will need to update the `--build-arge IMAGE=` argument passed to the
> `image.airbyte-nginx-unprivileged` task in the [Makefile](../../Makefile)

This [Dockerfile](Dockerfile) is a copy of the [unprivileged nginx alpine-slim Dockerfile](https://github.com/nginxinc/docker-nginx-unprivileged/blob/main/mainline/alpine-slim/Dockerfile).

We want all of Airbyte to run as the same uid/gid which requires us to build our own nginx image.  The nginx provided image uses uid/guid 101/101, and
we want them to be 1000/1000, which requires the `build-arg` values of `UID` and `GID` to be defined.

See [the docker Makefile](../../Makefile) for where these two `build-arg` values are defined.
