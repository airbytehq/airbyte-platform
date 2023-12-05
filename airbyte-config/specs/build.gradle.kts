import de.undercouch.gradle.tasks.download.Download

plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("de.undercouch.download") version "5.4.0"
}

dependencies {
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    api(libs.bundles.micronaut.annotation)

    implementation("commons-cli:commons-cli:1.4")
    implementation(libs.commons.io)
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.13.0"))
    implementation(libs.bundles.jackson)
    implementation(libs.google.cloud.storage)
    implementation(libs.micronaut.cache.caffeine)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-config:config-models"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-json-validation"))
    implementation(libs.okhttp)

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation("com.squareup.okhttp3:mockwebserver:4.9.1")
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)
    testImplementation(libs.junit.pioneer)
}

airbyte {
    spotless {
        excludes = listOf(
                "src/main/resources/seed/oss_registry.json",
                "src/main/resources/seed/local_oss_registry.json",
        )
    }
}

val downloadConnectorRegistry = tasks.register<Download>("downloadConnectorRegistry") {
    src("https://connectors.airbyte.com/files/registries/v0/oss_registry.json")
    dest(File(projectDir, "src/main/resources/seed/local_oss_registry.json"))
    overwrite( true)
}

tasks.processResources {
    dependsOn(downloadConnectorRegistry)
}
