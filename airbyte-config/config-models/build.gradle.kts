import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jsonschema2pojo.SourceType

plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("com.github.eirnym.js2p")
    kotlin("jvm")
}

dependencies {
    annotationProcessor(libs.bundles.micronaut.annotation.processor)
    api(libs.bundles.micronaut.annotation)

    implementation(platform("com.fasterxml.jackson:jackson-bom:2.13.0"))
    implementation(libs.bundles.jackson)
    implementation(libs.spotbugs.annotations)
    implementation(libs.guava)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    implementation(libs.google.cloud.storage)
    implementation(libs.aws.java.sdk.s3)
    implementation(libs.aws.java.sdk.sts)
    implementation(libs.s3)
    implementation(libs.sts)
    implementation(libs.bundles.apache)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(project(":airbyte-json-validation"))
    implementation(libs.airbyte.protocol)
    implementation(libs.commons.io)
    implementation(project(":airbyte-commons"))

    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.pioneer)
}

jsonSchema2Pojo {
    setSourceType(SourceType.YAMLSCHEMA.name)
    setSource(files("${sourceSets["main"].output.resourcesDir}/types"))
    targetDirectory = file("$buildDir/generated/src/gen/java/")

    targetPackage = "io.airbyte.config"
    useLongIntegers = true

    removeOldOutput = true

    generateBuilders = true
    includeConstructors = false
    includeSetters = true
    serializable = true
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("log4j2-config", "logger-client")
    }
}

tasks.named("compileKotlin") {
    dependsOn(tasks.named("generateJsonSchema2Pojo"))
}

tasks.register<Test>("log4j2IntegrationTest") {
    useJUnitPlatform {
        includeTags("log4j2-config")
    }
    testLogging {
        events = setOf(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)
    }
}

tasks.register<Test>("logClientsIntegrationTest") {
    useJUnitPlatform {
        includeTags("logger-client")
    }
    testLogging {
        events = setOf(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)
    }
}
