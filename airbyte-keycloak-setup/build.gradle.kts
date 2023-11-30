import java.util.Properties

plugins {
    id("io.airbyte.gradle.jvm.app")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

dependencies {
    annotationProcessor(platform(libs.micronaut.bom))
    annotationProcessor(libs.bundles.micronaut.annotation.processor)

    implementation( platform(libs.micronaut.bom))
    implementation( libs.bundles.micronaut)
    implementation( libs.bundles.keycloak.client)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-commons-auth"))
    implementation(project(":airbyte-commons-micronaut"))

    testAnnotationProcessor(platform(libs.micronaut.bom))
    testAnnotationProcessor(libs.bundles.micronaut.test.annotation.processor)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    testImplementation(libs.bundles.micronaut.test)
    testImplementation(libs.bundles.junit)
    testImplementation(libs.junit.jupiter.system.stubs)
}

val env = Properties().apply {
    load(rootProject.file(".env.dev").inputStream())
}
airbyte {
    application {
        mainClass = "io.airbyte.keycloak.setup.Application"
        defaultJvmArgs = listOf("-XX:+ExitOnOutOfMemoryError", "-XX:MaxRAMPercentage=75.0")
    }
    docker {
        imageName = "keycloak-setup"
    }
}
