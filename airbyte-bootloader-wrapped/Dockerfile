# This Dockerfile overrides an OSS base image. It adds a Cloud jar containing override beans to the
# base OSS classpath. It uses the base OSS image's entrypoint script to run the OSS application
# with no other changes.

ARG VERSION=dev-8c5398d930
FROM airbyte/bootloader:${VERSION}

# OSS app name that this Dockerfile overrides. Used to update the CLASSPATH in the OSS App's entrypoint script
ARG OSS_APP=airbyte-bootloader

# Cloud app name that represents the overridden OSS App
ARG CLOUD_APP=airbyte-bootloader-wrapped

# top-level package directory that all Cloud jars begin with
ARG CLOUD_JAR_BASE=io.airbyte-cloud

# jar file that contains override beans to replace OSS beans
ARG CLOUD_OVERRIDE_JAR=${CLOUD_JAR_BASE}-${CLOUD_APP}-${VERSION}.jar

# Directory in the base OSS image containing all base OSS jars and entrypoint script.
# The java application's $APP_HOME resolves to this directory.
ARG OSS_APP_HOME=${OSS_APP}-${VERSION}

# This is the main sed argument to modify the CLASSPATH inside the OSS entrypoint script.
# After escaping and arg interpolation, the end result is:
# 's/CLASSPATH=\$APP_HOME(.*)$/CLASSPATH=\$APP_HOME\1:\$APP_HOME\/lib\/override\/cloud-override-jar-name.jar/g'
# In this result, \1 is a back-reference to the first regex capture group, so this command appends the
# cloud override jar to the end of the CLASSPATH inside the target file.
ARG SED_SCRIPT="s/CLASSPATH=\\\$APP_HOME(.*)\$/CLASSPATH=\\\$APP_HOME\1:\\\$APP_HOME\/lib\/override\/${CLOUD_OVERRIDE_JAR}/g"

# add `$APP_HOME/lib/override/<cloud-override-jar>` to CLASSPATH in the OSS App's entrypoint script
# -E enables regex, and -i.old performs an in-place edit of the target file, with '.old' as a temporary backup file
# some versions of sed require a backup file to be specified for in-place edits.
RUN sed -Ei.old ${SED_SCRIPT} ${OSS_APP_HOME}/bin/${OSS_APP}

# copy the Cloud jar into the lib/override directory
COPY lib/${CLOUD_OVERRIDE_JAR} ${OSS_APP_HOME}/lib/override/

# no ENTRYPOINT specified in this Dockerfile, the base image's ENTRYPOINT will be used by default.
