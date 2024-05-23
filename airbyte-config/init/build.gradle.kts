plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
  id("org.jetbrains.kotlin.jvm")
  id("org.jetbrains.kotlin.kapt")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
  api(libs.bundles.micronaut.annotation)

  kapt(platform(libs.micronaut.platform))
  kapt(libs.bundles.micronaut.annotation.processor)

  implementation(project(":airbyte-commons"))
  implementation("commons-cli:commons-cli:1.4")
  implementation(project(":airbyte-config:specs"))
  implementation(project(":airbyte-config:config-models"))
  implementation(project(":airbyte-config:config-persistence"))
  implementation(project(":airbyte-data"))
  implementation(project(":airbyte-featureflag"))
  implementation(project(":airbyte-notification"))
  implementation(project(":airbyte-metrics:metrics-lib"))
  implementation(project(":airbyte-persistence:job-persistence"))
  implementation(libs.airbyte.protocol)
  implementation(project(":airbyte-json-validation"))
  implementation(libs.guava)
  implementation(libs.okhttp)
  implementation(libs.bundles.jackson)

  testImplementation(project(":airbyte-test-utils"))
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.bundles.junit)
  testImplementation(libs.assertj.core)
  testImplementation(libs.junit.pioneer)
  testImplementation(libs.mockk)

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

tasks.named("dockerCopyDistribution") {
  dependsOn(copyScripts)
}

tasks.processResources {
  from("${project.rootDir}/airbyte-connector-builder-resources")
}
