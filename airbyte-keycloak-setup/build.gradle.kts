plugins {
  id("io.airbyte.gradle.jvm.app")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

dependencies {

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.platform))
  implementation(libs.bundles.micronaut)
  implementation(libs.bundles.keycloak.client)
  implementation(libs.kotlin.logging) // Explicitly needed for KotlinLogging (was leaked through api() before)

  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-auth"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-commons-micronaut-security"))
  implementation(project(":oss:airbyte-commons-storage"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-db:db-lib"))
  implementation(project(":oss:airbyte-db:jooq"))

  runtimeOnly(libs.bundles.logback)

  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.junit.jupiter.system.stubs)
  testImplementation(libs.mockk)
  testImplementation(libs.platform.testcontainers.postgresql)

  testImplementation(project(":oss:airbyte-test-utils"))
}

airbyte {
  application {
    mainClass = "io.airbyte.keycloak.setup.ApplicationKt"
  }
  docker {
    imageName = "keycloak-setup"
  }
}

val copyScripts =
  tasks.register<Copy>("copyScripts") {
    from("scripts")
    into("build/airbyte/docker/")
  }

tasks.named("dockerCopyDistribution") {
  dependsOn(copyScripts)
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java dependencies.
// Once the code has been migrated to kotlin, this can also be removed.
tasks.withType<Jar>().configureEach {
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
