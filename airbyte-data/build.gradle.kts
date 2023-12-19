plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.kapt")
}

dependencies {
    api(libs.bundles.micronaut.annotation)

    kapt(platform(libs.micronaut.bom))
    kapt(libs.bundles.micronaut.annotation.processor)

    kaptTest(platform(libs.micronaut.bom))
    kaptTest(libs.bundles.micronaut.test.annotation.processor)

    implementation(libs.bundles.apache)
    implementation(libs.bundles.jackson)
    implementation(libs.bundles.micronaut.data.jdbc)
    implementation(libs.guava)
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-protocol"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-secrets"))
    implementation(project(":airbyte-db:db-lib"))
    implementation(project(":airbyte-db:jooq"))
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-featureflag"))
    implementation(libs.airbyte.protocol)

    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.postgresql)
    testImplementation(libs.platform.testcontainers.postgresql)
    testImplementation(libs.mockk)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
}

// Even though Kotlin is excluded on Spotbugs, this project
// still runs into spotbug issues. Working theory is that
// generated code is being picked up. Disable as a short-term fix.
tasks.named("spotbugsMain") {
    enabled = false
}
