plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
}

configurations.all {
    resolutionStrategy {
        // Ensure that the versions defined in deps.toml are used
        // instead of versions from transitive dependencies
        force(libs.flyway.core, libs.s3, libs.aws.java.sdk.s3)
    }
}
dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    annotationProcessor(libs.micronaut.jaxrs.processor)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.micronaut.inject)
    implementation(libs.micronaut.jaxrs.server)
    implementation(libs.micronaut.security)


    implementation(libs.flyway.core)
    implementation(libs.s3)
    implementation(libs.aws.java.sdk.s3)
    implementation(libs.sts)
    implementation(project(":airbyte-analytics"))
    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-auth"))
    implementation(project(":airbyte-commons-converters"))
    implementation(project(":airbyte-commons-license"))
    implementation(project(":airbyte-commons-temporal"))
    implementation(project(":airbyte-commons-temporal-core"))
    implementation(project(":airbyte-commons-with-dependencies"))
    implementation(project(":airbyte-config:init"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-config:config-secrets"))
    implementation(project(":airbyte-config:specs"))
    implementation(project(":airbyte-data"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(project(":airbyte-db:db-lib"))
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-oauth"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-persistence:job-persistence"))
    implementation(project(":airbyte-worker-models"))
    implementation(project(":airbyte-notification"))

    implementation(libs.bundles.apache)
    implementation(libs.slugify)
    implementation(libs.quartz.scheduler)
    implementation(libs.temporal.sdk)
    implementation(libs.swagger.annotations)
    implementation(libs.bundles.log4j)
    implementation(libs.commons.io)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation(project(":airbyte-test-utils"))
    testImplementation(libs.postgresql)
    testImplementation(libs.platform.testcontainers.postgresql)
    testImplementation(libs.mockwebserver)
    testImplementation(libs.mockito.inline)

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
}
