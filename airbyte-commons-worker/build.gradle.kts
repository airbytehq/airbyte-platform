plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    kotlin("jvm")
    kotlin("kapt")
}

configurations.all {
    resolutionStrategy {
        force(libs.platform.testcontainers.postgresql)
    }
}

dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)

    kapt(libs.bundles.micronaut.annotation.processor)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.bundles.micronaut.metrics)
    implementation(libs.micronaut.http)
    implementation(libs.kotlin.logging)
    implementation(libs.micronaut.kotlin.extensions)

    implementation(libs.bundles.kubernetes.client)
    implementation(libs.java.jwt)
    implementation(libs.gson)
    implementation(libs.guava)
    implementation(libs.temporal.sdk) {
        exclude(module = "guava")
    }
    implementation(libs.apache.ant)
    implementation(libs.apache.commons.text)
    implementation(libs.bundles.datadog)
    implementation(libs.commons.io)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    implementation(libs.bundles.apache)
    implementation(libs.bundles.log4j)
    implementation(libs.failsafe.okhttp)
    implementation(libs.google.cloud.storage)
    implementation(libs.okhttp)
    implementation(libs.aws.java.sdk.s3)
    implementation(libs.aws.java.sdk.sts)
    implementation(libs.s3)
    implementation(libs.sts)

    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-analytics"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-auth"))
    implementation(project(":airbyte-commons-converters"))
    implementation(project(":airbyte-commons-protocol"))
    implementation(project(":airbyte-commons-temporal"))
    implementation(project(":airbyte-commons-temporal-core"))
    implementation(project(":airbyte-commons-with-dependencies"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-config:config-secrets"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(project(":airbyte-persistence:job-persistence"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-worker-models"))
    implementation(libs.jakarta.validation.api)

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
    testAnnotationProcessor(libs.jmh.annotations)

    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.mockk)
    testImplementation(libs.json.path)
    testImplementation(libs.bundles.mockito.inline)
    testImplementation(libs.mockk)
    testImplementation(variantOf(libs.opentracing.util) { classifier("tests") })
    testImplementation(libs.postgresql)
    testImplementation(libs.platform.testcontainers.postgresql)
    testImplementation(libs.jmh.core)
    testImplementation(libs.jmh.annotations)
    testImplementation(libs.docker.java)
    testImplementation(libs.docker.java.transport.httpclient5)
    testImplementation(libs.reactor.test)
    testImplementation(libs.mockk)

    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
    testImplementation(libs.mockk)
}

tasks.named<Test>("test") {
    maxHeapSize = "6g"

    useJUnitPlatform {
        excludeTags("cloud-storage")
    }
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java _with_ lombok dependencies.)
// Kapt, by default, runs all annotation(processors and disables annotation(processing by javac, however)
// this default behavior(breaks the lombok java annotation(processor.  To avoid(lombok breaking, kapt(has)
// keepJavacAnnotationProcessors enabled, which causes duplicate META-INF files to be generated.)
// Once lombok has been removed, this can also be removed.)
tasks.withType<Jar>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
