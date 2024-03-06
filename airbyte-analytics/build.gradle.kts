plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.kapt")
}

dependencies {
    kapt(platform(libs.micronaut.bom))
    kapt(libs.bundles.micronaut.annotation.processor)

    api(libs.segment.java.analytics)
    api(libs.micronaut.http)
    api(libs.bundles.micronaut.annotation)
    api(libs.micronaut.kotlin.extensions)
    api(libs.kotlin.logging)
    api(project(":airbyte-commons"))
    api(project(":airbyte-config:config-models"))
    api(project(":airbyte-api"))


    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
    testImplementation(libs.mockk)
    testImplementation(libs.kotlin.test.runner.junit5)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

// This is a workaround related to kaptBuild errors.
// TODO: this should be removed when we move to kotlin 1.9.20
// TODO: we should write tests
afterEvaluate {
    tasks.named("kaptGenerateStubsTestKotlin") {
        enabled = false
    }
}
