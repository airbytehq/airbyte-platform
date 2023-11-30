plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    kotlin("jvm")
}

dependencies {
    implementation(libs.bundles.temporal)
    implementation(libs.failsafe)

    // We do not want dependency on(databases from this library.)
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-metrics:metrics-lib"))

    testImplementation(libs.assertj.core)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.junit.pioneer)
    testImplementation(libs.mockito.inline)
    testImplementation(libs.temporal.testing)
    testRuntimeOnly(libs.junit.jupiter.engine)
}
