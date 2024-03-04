plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    `java-test-fixtures`
}

configurations.all {
    exclude(group = "io.micronaut.flyway")
}

dependencies {
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    api(libs.bundles.micronaut.annotation)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-protocol"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:specs"))
    implementation(project(":airbyte-data"))
    implementation(project(":airbyte-db:db-lib"))
    implementation(project(":airbyte-db:jooq"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-json-validation"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(libs.bundles.apache)
    implementation(libs.google.cloud.storage)
    implementation(libs.commons.io)

    testImplementation("org.hamcrest:hamcrest-all:1.3")
    testImplementation(libs.platform.testcontainers.postgresql)
    testImplementation(libs.flyway.core)
    testImplementation(libs.mockito.inline)
    testImplementation(project(":airbyte-test-utils"))

    integrationTestImplementation(project(":airbyte-config:config-persistence"))

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    testFixturesApi(libs.jackson.databind)
    testFixturesApi(libs.guava)
    testFixturesApi(project(":airbyte-json-validation"))
    testFixturesApi(project(":airbyte-commons"))
    testFixturesApi(project(":airbyte-config:config-models"))
    testFixturesApi(project(":airbyte-config:config-secrets"))
    testFixturesApi(libs.airbyte.protocol)
    testFixturesApi(libs.lombok)
    testFixturesAnnotationProcessor(libs.lombok)
}
