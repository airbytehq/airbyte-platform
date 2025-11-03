import io.airbyte.gradle.extensions.AirbyteJvmExtension
import org.gradle.api.logging.LogLevel
import org.gradle.api.tasks.bundling.Tar
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.kotlin.dsl.assign
import java.io.FileOutputStream

val airbyteGradleVersion = "0.75.0"
// uncomment for testing plugin locally
// val airbyteGradleVersion = "local-test"

// The buildscript block defines dependencies in order for .gradle file evaluation.
// This is separate from application dependencies.
// See https://stackoverflow.com/questions/17773817/purpose-of-buildscript-block-in-gradle.
buildscript {
  repositories {
    maven {
      url = uri("https://plugins.gradle.org/m2/")
    }
  }
  dependencies {
//        classpath("com.bmuschko:gradle-docker-plugin:8.0.0")
//        // 6.x version of OpenApi generator is only compatible with jackson-core 2.13.x onwards.
//        // This conflicts with the jackson depencneis the bmuschko plugin is pulling in.
//        // Since api generation is only used in the airbyte-api module and the base gradle files
//        // are loaded in first, Gradle is not able to intelligently resolve this before loading in
//        // the bmuschko plugin and thus placing an older jackson version on the class path.
//        // The alternative is to import the openapi plugin for all modules.
//        // This might need to be updated when we change openapi plugin versions.
//        // classpath("com.fasterxml.jackson.core:jackson-core:2.13.0")
//
    classpath("org.codehaus.groovy:groovy-yaml:3.0.3")
  }
}

plugins {
  id("base")
  id("com.dorongold.task-tree") version "2.1.1"

  id("io.airbyte.gradle.jvm") version "0.75.0" apply false
  id("io.airbyte.gradle.jvm.app") version "0.75.0" apply false
  id("io.airbyte.gradle.jvm.lib") version "0.75.0" apply false
  id("io.airbyte.gradle.docker") version "0.75.0" apply false
  id("io.airbyte.gradle.publish") version "0.75.0" apply false
  id("io.airbyte.gradle.kube-reload") version "0.75.0" apply false

  id("com.github.eirnym.js2p") version "1.0" apply false
  id("org.openapi.generator") version "7.10.0" apply false
}

repositories {
  mavenCentral()
  maven {
    url = uri("https://airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars/")
  }
  maven {
    name = "cloudrepo"
    url = uri("https://airbyte.mycloudrepo.io/repositories/airbyte-public-jars")
    credentials {
      username = System.getenv("CLOUDREPO_USER")
      password = System.getenv("CLOUDREPO_PASSWORD")
    }
  }
}

if (System.getProperty("ciMode", "false") == "true") {
  gradle.startParameter.logLevel = LogLevel.QUIET

  val logFile = FileOutputStream("gradle.log", true)
  with(logging) {
    addStandardOutputListener { logFile.write(it.toString().toByteArray()) }
    addStandardErrorListener { logFile.write(it.toString().toByteArray()) }
  }

  allprojects {
    tasks.withType<JavaCompile> {
      options.isDeprecation = false
      options.isWarnings = false
    }
  }
}

extra.apply {
  // used for publishing jars
  set("oss_version", System.getenv("VERSION"))
  // todo (cgardens) - remove the implicit behavior so it doesn't require an explanatory comment.
  // for cloud (prod + stage) this is going to be an image_tag (see build-artifacts.yaml). for OSS this is going to be the oss version.
  set("webapp_version", System.getenv("VERSION") ?: "dev")
}

val ossVersion = extra["oss_version"]

allprojects {
  // by default gradle uses directory as the project name. That works very well in a single project environment but
  // projects clobber each other in an environments with subprojects when projects are in directories named identically.
  val sub =
    rootDir
      .toPath()
      .relativize(projectDir.parentFile.toPath())
      .toString()
      .replace("/", ".")
  group = "io.airbyte${if (sub.isEmpty()) "" else ".$sub"}"

  // todo (cgardens) - separate oss version and artifact version
  version = ossVersion ?: "dev"

  pluginManager.withPlugin("io.airbyte.gradle.jvm") {
    configure<AirbyteJvmExtension> {
      pmd {
        ruleFiles = rootProject.files("./oss/pmd-rules.xml")
      }
    }
    repositories {
      gradlePluginPortal()
      mavenCentral()
    }
    dependencies {
      "pmd"(project(":oss:airbyte-pmd-rules"))
    }
  }
  configurations.all {
    resolutionStrategy {
      // Ensure that the versions defined in deps.toml are used
      // instead of versions from transitive dependencies
      // Force to avoid updated version brought in transitively from Micronaut 3.8+
      force(
        libs.flyway.core,
        libs.jooq,
        libs.s3,
        libs.aws.java.sdk.s3,
        libs.sts,
        libs.aws.java.sdk.sts,
        libs.elasticsearch,
        libs.apache.mime4j.core, // Force upgrade to remediate GHSA-jw7r-rxff-gv24
        libs.platform.testcontainers.postgresql,
      )
    }
  }
}

// For internal use we call this from the root of the project. In the project
// mirrored to open-source, this project is the root. That means that its name
// is different by default. In the internal repo it is project(:oss) and in
// oss it is the rootProject. We create this closure and then expose it to all
// submodules so that they have a canonical way of referring to this gradle
// module.
val ossRootActual = project
subprojects {
  extra.apply {
    set("ossRootProject", ossRootActual)
  }
}

tasks.register<Tar>("archiveReports") {
  dependsOn(subprojects.flatMap { it.getTasksByName("checkstyleMain", true) })
  dependsOn(subprojects.flatMap { it.getTasksByName("checkstyleTest", true) })
  dependsOn(subprojects.flatMap { it.getTasksByName("jacocoTestReport", true) })
  dependsOn(subprojects.flatMap { it.getTasksByName("pmdMain", true) })
  dependsOn(subprojects.flatMap { it.getTasksByName("pmdTest", true) })
  dependsOn(subprojects.flatMap { it.getTasksByName("test", true) })
  dependsOn(subprojects.flatMap { it.getTasksByName("checkstyleAcceptanceTests", true) })
  dependsOn(subprojects.flatMap { it.getTasksByName("pmdAcceptanceTests", true) })

  archiveFileName.set("${project.name}-reports.tar")
  destinationDirectory.set(layout.buildDirectory.dir("dist"))

  // Collect reports from each subproject
  subprojects.forEach { subproject ->
    from("${subproject.layout.buildDirectory.get()}/reports") {
      into("${subproject.name}/reports")
    }
  }
}

// todo (cgardens) - move this into the plugin.
// By default ./gradlew :oss:build doesn't do anything, so we need to manually set the dependencies.
val taskNames = listOf("assemble", "build", "check", "clean", "format", "jar", "publishToMavenLocal", "test", "integrationTest")
// For tasks that don't exist gradle by default, we need to register them first.
tasks.register("format")
tasks.register("jar")
tasks.register("publishToMavenLocal")
tasks.register("test")
tasks.register("integrationTest")
// For each task type register its subprojects instance of that task type as a dependency. It is
// equivalent to doing something like this:
taskNames.forEach { taskName ->
  tasks.named(taskName) {
    dependsOn(subprojects.flatMap { it.getTasksByName(taskName, true) })
  }
}
