plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.kapt")
}

dependencies {
    kapt(libs.bundles.micronaut.annotation.processor)

    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(libs.okhttp)
    implementation("org.apache.httpcomponents:httpclient:4.5.13")
    implementation("org.commonmark:commonmark:0.21.0")

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(libs.guava)
    implementation(libs.bundles.apache)
    implementation(libs.commons.io)
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.13.0"))
    implementation(libs.bundles.jackson)
    // TODO remove this, it"s used for String.isEmpty check)
    implementation(libs.bundles.log4j)

    testImplementation(libs.mockk)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
    testImplementation(libs.mockito.inline)
    testImplementation(libs.mockwebserver)
}
