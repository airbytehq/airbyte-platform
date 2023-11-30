plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
}

dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)

    implementation(libs.apache.commons.text)

    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-persistence:job-persistence"))
    implementation(libs.airbyte.protocol)
    implementation(libs.guava)
    implementation(libs.slf4j.api)
    implementation(libs.bundles.datadog)

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
    testAnnotationProcessor(libs.jmh.annotations)

    testImplementation(libs.bundles.micronaut.test)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}
