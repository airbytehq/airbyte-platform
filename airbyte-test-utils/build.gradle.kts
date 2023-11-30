plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
}

configurations.all {
    exclude( group = "io.micronaut.jaxrs")
    exclude( group = "io.micronaut.sql")

    resolutionStrategy {
        // Force to avoid(updated version(brought in transitively from Micronaut)
        force(libs.platform.testcontainers.postgresql)
    }
}

dependencies {
    api(project(":airbyte-db:db-lib"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-commons-temporal"))
    implementation(project(":airbyte-commons-worker"))

    implementation(libs.bundles.kubernetes.client)
    implementation(libs.temporal.sdk)

    api(libs.junit.jupiter.api)

    // Mark as compile only(to avoid leaking transitively to connectors
    compileOnly(libs.platform.testcontainers.postgresql)

    testImplementation(libs.platform.testcontainers.postgresql)

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}
