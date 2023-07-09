import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

// @Suppress can be removed when KTIJ-19369 has been fixed, or when we upgrade to gradle 8.1
@Suppress("DSL_SCOPE_VIOLATION")
plugins {
  `java-library`
  `maven-publish`
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.kapt)
}

dependencies {
  kapt(platform(libs.micronaut.bom))
  kapt(libs.bundles.micronaut.annotation.processor)

  implementation(platform(libs.micronaut.bom))
  implementation(libs.micronaut.inject)
  implementation(libs.launchdarkly)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.dataformat)
  implementation(libs.jackson.kotlin)

  kaptTest(platform(libs.micronaut.bom))
  kaptTest(libs.bundles.micronaut.test.annotation.processor)

  testImplementation(kotlin("test"))
  testImplementation(kotlin("test-junit5"))
  testImplementation(libs.bundles.micronaut.test)
  testImplementation(libs.mockk)
  testImplementation(libs.bundles.junit)
}

tasks.withType<KotlinCompile> {
  compilerOptions {
    jvmTarget.set(JvmTarget.JVM_17)
  }
}

tasks.test {
  useJUnitPlatform()
}

publishing {
  repositories {
    publications {
      create<MavenPublication>("${project.name}") {
        groupId = "${project.group}"
        artifactId = "${project.name}"
        version = "${rootProject.version}"
        repositories.add(rootProject.repositories.getByName("cloudrepo"))
        from(components["java"])
      }
    }
  }
}
