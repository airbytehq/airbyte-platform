import org.jsonschema2pojo.SourceType

plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.publish")
}

dependencies {
  implementation(platform(libs.fasterxml))
  implementation(libs.bundles.jackson)
  implementation(project(":oss:airbyte-commons"))
  implementation(project(":oss:airbyte-commons-micronaut"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(libs.airbyte.protocol)
  implementation(libs.jakarta.annotation.api)  // Needed for generated @Generated annotations
}

sourceSets {
  main {
    java {
      srcDir("${project.layout.projectDirectory}/src/generated/java")
    }
  }
}
