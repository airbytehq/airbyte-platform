plugins {
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

airbyte {
    docker {
        imageName = "keycloak"
    }
}

val copyTheme = tasks.register<Copy>("copyTheme") {
    from("themes")
    into("build/airbyte/docker/bin/themes")
}

val copyScripts = tasks.register<Copy>("copyScripts") {
    from("scripts")
    into("build/airbyte/docker/bin/scripts")
}

tasks.named("dockerBuildImage") {
    dependsOn(copyScripts, copyTheme)
}
