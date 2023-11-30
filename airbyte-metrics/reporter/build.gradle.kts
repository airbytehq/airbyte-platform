plugins {
    id("io.airbyte.gradle.jvm.app")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

configurations {
    create("jdbc")
}

configurations.all {
    resolutionStrategy {
        force (libs.jooq)
    }
}

dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)

    implementation(platform(libs.micronaut.bom))
    implementation(libs.bundles.micronaut)

    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-db:jooq"))
    implementation(project(":airbyte-db:db-lib"))
    implementation(project(":airbyte-metrics:metrics-lib"))
    implementation(libs.jooq)

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)

    testImplementation(project(":airbyte-test-utils"))
    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.postgresql)
    testImplementation(libs.platform.testcontainers.postgresql)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}

airbyte {
    application {
        name = "airbyte-metrics-reporter"
        mainClass = "io.airbyte.metrics.reporter.Application"
        defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    }
    docker {
        imageName = "metrics-reporter"
    }
}
