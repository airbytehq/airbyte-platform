plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

dependencies {
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    api(libs.bundles.micronaut.annotation)

    implementation(project(":airbyte-commons"))
    implementation("commons-cli:commons-cli:1.4")
    implementation(project(":airbyte-config:specs"))
    implementation(project(":airbyte-config:config-models"))
    implementation(project(":airbyte-config:config-persistence"))
    implementation(project(":airbyte-featureflag"))
    implementation(project(":airbyte-notification"))
    implementation(project(":airbyte-persistence:job-persistence"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-json-validation"))
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    implementation(libs.guava)

    testImplementation(project(":airbyte-test-utils"))
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)
}

airbyte {
    docker {
        imageName = "init"
    }
}

val copyScripts = tasks.register<Copy>("copyScripts") {
    from("scripts")
    into("build/airbyte/docker/bin/scripts")
}

tasks.named("dockerBuildImage") {
    dependsOn(copyScripts)
}

tasks.processResources {
    from("${project.rootDir}/airbyte-connector-builder-resources")
}
