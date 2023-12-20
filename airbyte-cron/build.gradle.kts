import java.util.Properties

plugins {
    id("io.airbyte.gradle.jvm.app")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
    kotlin("jvm")
    kotlin("kapt")
}

dependencies {
    annotationProcessor(libs.lombok)
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    kapt(libs.bundles.micronaut.annotation.processor)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)
    implementation(libs.bundles.micronaut.metrics)
    implementation(libs.bundles.kubernetes.client)
    implementation(libs.bundles.temporal)
    implementation(libs.bundles.datadog)
    implementation(libs.failsafe)
    implementation(libs.failsafe.okhttp)
    implementation(libs.java.jwt)
    implementation(libs.kotlin.logging)
    implementation(libs.okhttp)
    implementation(libs.sentry.java)
    implementation(libs.lombok)
    implementation(libs.commons.io)

    implementation(project(":airbyte-api"))
    implementation(project(":airbyte-analytics"))
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-auth"))
    implementation(project(":airbyte-commons-micronaut"))
    implementation(project(":airbyte-commons-temporal"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-config:init"))
    implementation(project(":airbyte-json-validation"))
    implementation(project(":airbyte-data"))
    implementation(project(":airbyte-db:db-lib"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(project(":airbyte-persistence:job-persistence"))


    testImplementation(libs.bundles.junit)
    testImplementation(libs.mockk)
}

val env = Properties().apply {
    load(rootProject.file(".env.dev").inputStream())
}

airbyte {
    application {
        mainClass = "io.airbyte.cron.MicronautCronRunner"
        defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
        @Suppress("UNCHECKED_CAST")
        localEnvVars.putAll(env.toMap() as Map<String, String>)
        localEnvVars.putAll(mapOf(
            "AIRBYTE_ROLE"   to (System.getenv("AIRBYTE_ROLE") ?: "undefined"),
            "AIRBYTE_VERSION" to env["VERSION"].toString(),
        ))
    }

    docker {
        imageName = "cron"
    }
}

// The DuplicatesStrategy will be required while this module is mixture of kotlin and java _with_ lombok dependencies.)
// Kapt, by default, runs all annotation(processors and disables annotation(processing by javac, however)
// this default behavior breaks the lombok java annotation(processor.  To avoid(lombok breaking, kapt has)
// keepJavacAnnotationProcessors enabled, which causes duplicate META-INF files to be generated.)
// Once lombok has been removed, this can also be removed.)
tasks.withType<Jar>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
