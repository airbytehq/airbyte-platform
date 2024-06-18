plugins {
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

airbyte {
  docker {
    imageName = "keycloak"
  }
}

val copyPasswordBlacklists = tasks.register<Copy>("copyPasswordBlacklists") {
  from("password-blacklists")
  into("build/airbyte/docker/bin/password-blacklists")
}

val copyTheme = tasks.register<Copy>("copyTheme") {
  from("themes")
  into("build/airbyte/docker/bin/themes")
}

val copyScripts = tasks.register<Copy>("copyScripts") {
  from("scripts")
  into("build/airbyte/docker/bin/scripts")
}

tasks.named("dockerCopyDistribution") {
  dependsOn(copyPasswordBlacklists, copyScripts, copyTheme)
}
