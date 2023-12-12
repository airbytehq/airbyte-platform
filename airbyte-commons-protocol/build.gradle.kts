plugins {
    id("io.airbyte.gradle.jvm")
    id("io.airbyte.gradle.publish")
}

dependencies {
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

    implementation(libs.bundles.micronaut.annotation)
    testImplementation(libs.bundles.micronaut.test)

    implementation(project(":airbyte-commons"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-json-validation"))
    implementation(libs.guava)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(libs.bundles.jackson)

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}
