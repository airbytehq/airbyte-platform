plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
}

dependencies {
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.13.0"))
    implementation(libs.bundles.jackson)
    implementation(libs.guava)
    implementation(libs.google.cloud.storage)
    implementation(libs.bundles.apache)
    implementation(libs.appender.log4j2)
    implementation(libs.aws.java.sdk.s3)
    implementation(libs.aws.java.sdk.sts)

    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-json-validation"))
    implementation(libs.airbyte.protocol)

    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
}
