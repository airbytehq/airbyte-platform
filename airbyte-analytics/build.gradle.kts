plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.kapt")
}

dependencies {
    kapt(libs.bundles.micronaut.annotation.processor)

    api(libs.segment.java.analytics)
    api(libs.micronaut.http)
    api(libs.bundles.micronaut.annotation)
    api(libs.micronaut.kotlin.extensions)
    api(libs.kotlin.logging)
    api(project(":airbyte-commons"))
    api(project(":airbyte-config:config-models"))
    api(project(":airbyte-api"))

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
    testImplementation(libs.mockk)
    testImplementation(libs.kotlin.test.runner.junit5)
}
