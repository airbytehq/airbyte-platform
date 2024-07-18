plugins {
  id("io.airbyte.gradle.jvm.lib")
  id("io.airbyte.gradle.docker")
  id("io.airbyte.gradle.publish")
}

dependencies {
  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)     // Lombok must be added BEFORE Micronaut
  api(libs.bundles.micronaut.annotation)

  ksp(platform(libs.micronaut.platform))
  ksp(libs.bundles.micronaut.annotation.processor)

  implementation(project(":oss:airbyte-commons"))
  implementation("commons-cli:commons-cli:1.4")
  implementation(project(":oss:airbyte-config:specs"))
  implementation(project(":oss:airbyte-config:config-models"))
  implementation(project(":oss:airbyte-config:config-persistence"))
  implementation(project(":oss:airbyte-data"))
  implementation(project(":oss:airbyte-featureflag"))
  implementation(project(":oss:airbyte-notification"))
  implementation(project(":oss:airbyte-metrics:metrics-lib"))
  implementation(project(":oss:airbyte-persistence:job-persistence"))
  implementation(libs.airbyte.protocol)
  implementation(project(":oss:airbyte-json-validation"))
  implementation(libs.guava)
  implementation(libs.okhttp)
  implementation(libs.bundles.jackson)

  testImplementation(project(":oss:airbyte-test-utils"))
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
  from("${(ext["ossRootProject"] as Project).projectDir}/airbyte-connector-builder-resources")
}
