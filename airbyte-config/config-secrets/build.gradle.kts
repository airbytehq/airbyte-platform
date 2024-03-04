plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    `java-test-fixtures`
    kotlin("jvm")
    kotlin("kapt")
}

dependencies {
    kapt(platform(libs.micronaut.bom))
    kapt(libs.bundles.micronaut.annotation.processor)

    api(libs.bundles.micronaut.annotation)
    api(libs.micronaut.kotlin.extensions)
    api(libs.kotlin.logging)
    api(libs.slf4j.api)
    api(libs.bundles.log4j)
    api(libs.micronaut.jackson.databind)
    api(libs.google.cloud.storage)
    api(libs.micronaut.jooq)
    api(libs.guava)
    api(libs.bundles.secret.hydration)
    api(libs.airbyte.protocol)
    api(libs.jakarta.transaction.api)
    api(libs.micronaut.data.tx)
    api(libs.aws.java.sdk.sts)
    api(project(":airbyte-commons"))

    /*
     * Marked as "implementation" to avoid leaking these dependencies to services
     * that only use the retrieval side of the secret infrastructure.  The services
     * that do need these dependencies will already have them declared, as they will
     * need to define singletons from these modules in order for everything work.
     */
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-json-validation"))

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.mockk)
    testImplementation(libs.kotlin.test.runner.junit5)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.airbyte.protocol)
    testImplementation(libs.apache.commons.lang)
    testImplementation(libs.testcontainers.vault)
    testImplementation(testFixtures(project(":airbyte-config:config-persistence")))
}

// This is a workaround related to kaptBuild errors. It seems to be because there are no tests in cloud-airbyte-api-server.
// TODO: this should be removed when we move to kotlin 1.9.20
// TODO: we should write tests
afterEvaluate {
    tasks.named("kaptGenerateStubsTestKotlin") {
        enabled = false
    }
}
