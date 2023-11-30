import de.undercouch.gradle.tasks.download.Download

plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("de.undercouch.download") version "5.4.0"
}

dependencies {
    implementation(libs.bundles.jackson)
    implementation(libs.guava)
    implementation(libs.bundles.slf4j)
    implementation(libs.commons.io)
    implementation(libs.bundles.apache)
    implementation(libs.google.cloud.storage)
    implementation(libs.bundles.log4j)
    implementation(libs.airbyte.protocol)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    // this dependency is an exception to the above rule because it is only used INTERNALLY to the commons library.
    implementation("com.jayway.jsonpath:json-path:2.7.0")

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}

airbyte {
    spotless {
        excludes = listOf("src/main/resources/seed/specs_secrets_mask.yaml")
    }
}

val downloadSpecSecretMask = tasks.register<Download>("downloadSpecSecretMask") {
    src("https://connectors.airbyte.com/files/registries/v0/specs_secrets_mask.yaml")
    dest(File(projectDir, "src/main/resources/seed/specs_secrets_mask.yaml"))
    overwrite(true)
}

tasks.named("processResources") {
    dependsOn(downloadSpecSecretMask)
}
