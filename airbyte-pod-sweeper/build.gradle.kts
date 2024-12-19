plugins {
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

airbyte {
  docker {
    imageName = "pod-sweeper"
  }
}

val copyScripts = tasks.register<Copy>("copyScripts") {
  from("scripts")
  into("build/airbyte/docker/")
}

tasks.named("dockerCopyDistribution") {
  dependsOn(copyScripts)
}