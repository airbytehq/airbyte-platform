import org.jsonschema2pojo.SourceType

plugins {
    id("io.airbyte.gradle.jvm.lib")
    id("io.airbyte.gradle.publish")
    id("com.github.eirnym.js2p")
}

dependencies {
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.13.0"))
    implementation(libs.bundles.jackson)
    implementation(project(":airbyte-commons"))
    implementation(project(":airbyte-config:config-models"))
    implementation(libs.airbyte.protocol)
    implementation(project(":airbyte-api"))

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)
}

jsonSchema2Pojo {
    setSourceType(SourceType.YAMLSCHEMA.name)
    setSource(files("${sourceSets["main"].output.resourcesDir}/workers_models"))
    targetDirectory = file("$buildDir/generated/src/gen/java/")
    removeOldOutput = true

    targetPackage = "io.airbyte.persistence.job.models"

    useLongIntegers = true
    generateBuilders = true
    includeConstructors = false
    includeSetters = true
}
