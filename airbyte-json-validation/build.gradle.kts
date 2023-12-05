plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
}

dependencies {
    implementation(project(":airbyte-commons"))
    implementation(libs.guava)
    implementation("com.networknt:json-schema-validator:1.0.72")
    // needed so that we can follow $ref when parsing json. jackson does not support this natively.
    implementation("me.andrz.jackson:jackson-json-reference-core:0.3.2")

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)

    testImplementation(libs.junit.pioneer)
}
