plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
}

configurations.all {
    resolutionStrategy {
        force(libs.platform.testcontainers.postgresql)
    }
}

dependencies {
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.13.0"))
    implementation(libs.bundles.jackson)
    implementation(libs.spotbugs.annotations)
    implementation(libs.guava)
    implementation(libs.commons.io)
    implementation(libs.bundles.apache)
    // TODO: remove this, it's pulled in for a Strings.notEmpty() check
    implementation(libs.bundles.log4j)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-protocol"))
    implementation(project(":airbyte-oauth"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-db:jooq"))
    implementation(project(":airbyte-db:db-lib"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-notification"))
    implementation(project(":airbyte-analytics"))
    implementation(project(":airbyte-metrics:metrics-lib"))

    implementation(libs.sentry.java)
    implementation(libs.otel.semconv)
    implementation(libs.otel.sdk)
    implementation(libs.otel.sdk.testing)
    implementation(libs.micrometer.statsd)
    implementation(platform(libs.otel.bom))
    implementation("io.opentelemetry:opentelemetry-api")
    implementation("io.opentelemetry:opentelemetry-sdk")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp")
    implementation(libs.apache.commons.collections)
    implementation(libs.datadog.statsd.client)

    testImplementation(project(":airbyte-config:config-persistence"))
    testImplementation(project(":airbyte-test-utils"))
    testImplementation(libs.platform.testcontainers.postgresql)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}
