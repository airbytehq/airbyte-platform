plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

airbyte {
    docker {
        imageName = "temporal"
    }
}

val copyScripts = tasks.register<Copy>("copyScripts") {
    from("scripts")
    into("build/airbyte/docker/")
}

tasks.named("dockerBuildImage") {
    dependsOn(copyScripts)
}
