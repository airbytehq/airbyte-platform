plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    kotlin("jvm")
    kotlin("kapt")
}

dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)

    kapt(libs.bundles.micronaut.annotation.processor)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.failsafe.okhttp)
    implementation(libs.kotlin.logging)
    implementation(libs.okhttp)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(project(":airbyte-commons"))

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.mockito.inline)
    testImplementation(libs.mockk)
}

tasks.named<Test>("test") {
    maxHeapSize = "2g"
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java _with_ lombok dependencies.
// Kapt, by default, runs all annotation processors and disables annotation processing by javac, however
// this default behavior breaks the lombok java annotation processor.  To avoid lombok breaking, kapt has
// keepJavacAnnotationProcessors enabled, which causes duplicate META-INF files to be generated.
// Once lombok has been removed, this can also be removed.
tasks.withType<Jar>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
