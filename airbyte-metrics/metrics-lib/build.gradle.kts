plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    kotlin("jvm")
    kotlin("kapt")
}

dependencies {
    kapt(libs.bundles.micronaut.annotation.processor)

    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-db:jooq"))
    implementation(project(":airbyte-db:db-lib"))

    implementation(libs.guava)
    implementation(libs.google.cloud.storage)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(libs.otel.semconv)
    implementation(libs.otel.sdk)
    implementation(libs.otel.sdk.testing)
    implementation(libs.micrometer.statsd)
    implementation(platform(libs.otel.bom))
    implementation(("io.opentelemetry:opentelemetry-api"))
    implementation(("io.opentelemetry:opentelemetry-sdk"))
    implementation(("io.opentelemetry:opentelemetry-exporter-otlp"))

    implementation(libs.java.dogstatsd.client)
    implementation(libs.bundles.datadog)

    testImplementation(project(":airbyte-config:config-persistence"))
    testImplementation(project(":airbyte-test-utils"))
    testImplementation(libs.platform.testcontainers.postgresql)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.mockk)
    testImplementation((variantOf(libs.opentracing.util) { classifier("tests") }))

    testImplementation(libs.junit.pioneer)

}
