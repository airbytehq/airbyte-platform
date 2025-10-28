import org.jsonschema2pojo.SourceType

plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  annotationProcessor(libs.bundles.micronaut.annotation.processor)

  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(libs.bundles.micronaut.annotation)

  implementation(project(":oss:airbyte-json-validation"))
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-domain:models"))

  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.datadog)
  implementation(libs.bundles.jackson)
  implementation(libs.guava)
  implementation(libs.micronaut.kotlin.extension.functions)
  implementation(libs.airbyte.protocol)
  implementation(libs.kotlin.logging)
  implementation(libs.cron.utils)

  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.bundles.micronaut.test)

  testRuntimeOnly(libs.junit.jupiter.engine)
}

sourceSets {
  main {
    java {
      srcDir("${project.layout.projectDirectory}/src/generated/java")
    }
  }
}
