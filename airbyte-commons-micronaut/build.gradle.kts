plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
}

dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.micronaut.security)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-config:config-models"))

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.mockito.inline)
}

tasks.named<Test>("test") {
    maxHeapSize = "2g"
}
