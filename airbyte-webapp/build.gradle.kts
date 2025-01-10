
import com.github.gradle.node.NodeExtension
import com.github.gradle.node.pnpm.task.PnpmTask
import groovy.json.JsonSlurper
import java.io.FileReader

plugins {
    id("base")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.kube-reload")
    alias(libs.plugins.node.gradle)
}

ext {
    set("appBuildDir", "build/app")
}

/**
 * Utility function to parse a .gitignore file into a list of ignore pattern entries
 */
fun parseIgnoreFile(f: File): List<String> {
    val ignores = mutableListOf<String>()
    f.forEachLine { line ->
        //ignore comments and empty lines
        if (!line.startsWith('#') && line.isNotEmpty()) {
            ignores.add(line)
        }
    }
    return ignores
}


// Use the node version that's defined in the .nvmrc file
val nodeVersion = file("${projectDir}/.nvmrc").readText().trim()

// Read pnpm version to use from package.json engines.pnpm entry
val parsedJson = JsonSlurper().parse(FileReader("${projectDir}/package.json")) as Map<*, *>  // Cast to Map
val engines = parsedJson["engines"] as? Map<*, *>  // Safely cast to Map if 'engines' exists
val pnpmVer = engines?.get("pnpm")?.toString()?.trim()  // Extract 'pnpm' as String and trim

/**
 * A list of all files outside the webapp folder, that the webapp build depends on, i.e.
 * if those change we can't reuse a cached build.
 */
val outsideWebappDependencies = listOf(
    project(":oss:airbyte-api:server-api").file("src/main/openapi/config.yaml").path,
    project(":oss:airbyte-api:commons").file("src/main/openapi/cloud-config.yaml").path,
    project(":oss:airbyte-api:problems-api").file("src/main/openapi/api-problems.yaml").path,
    project(":oss:airbyte-connector-builder-server").file("src/main/openapi/openapi.yaml").path,
    project(":oss:airbyte-connector-builder-server").file("CDK_VERSION").path,
)

configure<NodeExtension> {
    download = true
    version = nodeVersion
    pnpmVersion = pnpmVer
    distBaseUrl = "https://nodejs.org/dist"
}

airbyte {
    docker {
        imageName = "webapp"
    }

    kubeReload {
        deployment = "ab-webapp"
        container = "airbyte-webapp-container"
    }
}

tasks.named("pnpmInstall") {
    /*
    Add patches folder to inputs of pnpmInstall task, since it has pnpm-lock.yml as an output
    thus wouldn't rerun in case a patch get changed
    */
    inputs.dir("patches")
}

// fileTree to watch node_modules, but exclude the .cache dir since that might have changes on every build
val nodeModules = fileTree("node_modules") {
    exclude(".cache")
}

/**
 * All files inside the webapp folder that aren't gitignored
 */
val allFiles = fileTree(".") {
    exclude(parseIgnoreFile(file("../.gitignore")))
    exclude(parseIgnoreFile(file(".gitignore")))
    exclude(parseIgnoreFile(file("./src/core/api/generated/.gitignore")))
    exclude(parseIgnoreFile(file("./src/core/api/types/.gitignore")))
}

tasks.register<PnpmTask>("pnpmBuild") {
    dependsOn(tasks.named("pnpmInstall"))

    // todo (cgardens) - this isn't great because this version is used for cloud as well (even though it's pulled from the oss project).
    environment.put("VERSION", (ext["ossRootProject"] as Project).ext["webapp_version"] as String)

    args = listOf("build")

    // The WEBAPP_BUILD_CLOUD_ENV environment variable is an input for this task, since it changes for which env we're building the webapp
    inputs.property("cloudEnv", System.getenv("WEBAPP_BUILD_CLOUD_ENV") ?: "")
    inputs.files(allFiles, outsideWebappDependencies)

    outputs.dir(project.ext.get("appBuildDir") as String)
}

tasks.register<PnpmTask>("test") {
    dependsOn(tasks.named("pnpmInstall"))
    
    args = listOf("run", "test:ci")
    inputs.files(allFiles, outsideWebappDependencies)

    /*
    The test has no outputs, thus we always treat the outputs up to date
    as long as the inputs have not changed
    */
    outputs.upToDateWhen { true }
}

tasks.register<PnpmTask>("cypress") {
    dependsOn(tasks.named("pnpmInstall"))

    /*
    If the cypressWebappKey property has been set from the outside via the workflow file
    we'll record the cypress session, otherwise we're not recording
    */
    val hasRecordingKey = !System.getenv("CYPRESS_RECORD_KEY").isNullOrEmpty()
    args = if (hasRecordingKey && System.getProperty("cypressRecord", "false") == "true") {
        val group = System.getenv("CYPRESS_GROUP") ?: "default-group"
        listOf("run", "cypress:run", "--record", "--group", group)
    } else {
        listOf("run", "cypress:run")
    }

    /*
    Mark the outputs as never up to date, to ensure we always run the tests.
    We want this because they are e2e tests and can depend on other factors e.g., external dependencies.
    */
    outputs.upToDateWhen { false }
}


tasks.register<PnpmTask>("cypressCloud") {
    dependsOn(tasks.named("pnpmInstall"))

    val hasRecordingKey = !System.getenv("CYPRESS_RECORD_KEY").isNullOrEmpty()
    args = if (hasRecordingKey && System.getProperty("cypressRecord", "false") == "true") {
        listOf("run", "cloud-test:stage", "--record")
    } else {
        listOf("run", "cloud-test:stage")
    }

    /*
    Mark the outputs as never up to date, to ensure we always run the tests.
    We want this because they are e2e tests and can depend on other factors e.g., external dependencies.
    */
    outputs.upToDateWhen { false }
}

tasks.register<PnpmTask>("licenseCheck") {
    dependsOn(tasks.named("pnpmInstall"))

    args = listOf("run", "license-check")

    inputs.file("package.json")
    inputs.file("pnpm-lock.yaml")
    inputs.file("scripts/license-check.js")

    outputs.upToDateWhen { true }
}

tasks.register<PnpmTask>("validateLock") {
    dependsOn(tasks.named("pnpmInstall"))

    args = listOf("run", "validate-lock")

    inputs.files(allFiles)

    outputs.upToDateWhen { true }
}

tasks.register<PnpmTask>("validateLinks") {
   dependsOn(tasks.named("pnpmInstall"))

   args = listOf("run", "validate-links")

   inputs.file("scripts/validate-links.ts")
   inputs.file("src/core/utils/links.ts")

   // Configure the up-to-date check to always run again, since it checks availability of URLs
   outputs.upToDateWhen { false }
}

tasks.register<PnpmTask>("unusedCode") {
    dependsOn(tasks.named("pnpmInstall"))

    args = listOf("run", "unused-code")

    inputs.files(allFiles, outsideWebappDependencies)

    outputs.upToDateWhen { true }
}

tasks.register<PnpmTask>("prettier") {
    dependsOn(tasks.named("pnpmInstall"))

    args = listOf("run", "prettier:ci")

    inputs.files(allFiles)

    outputs.upToDateWhen { true }
}

tasks.register<PnpmTask>("buildStorybook") {
    dependsOn(tasks.named("pnpmInstall"))

    args = listOf("run", "build:storybook")

    inputs.files(allFiles, outsideWebappDependencies)

    outputs.dir("build/storybook")

    environment = mapOf(
        "NODE_OPTIONS" to "--max_old_space_size=8192"
    )
}

tasks.register<Copy>("copyBuildOutput") {
    dependsOn(tasks.named("pnpmBuild"))

    from("${project.projectDir}/${project.ext.get("appBuildDir")}")
    into("build/airbyte/docker/bin/build")
}

tasks.register<Copy>("copyNginx") {
    from("${project.projectDir}/nginx")
    into("build/airbyte/docker/bin/nginx")
}

// Those tasks should be run as part of the "check" task
tasks.named("check") {
    dependsOn(tasks.named("licenseCheck"), tasks.named("validateLock"), tasks.named("unusedCode"), tasks.named("prettier"), tasks.named("test"))
}

// Some check tasks only should be run on CI, thus a separate ciCheck task
tasks.register("ciCheck") {
    dependsOn(tasks.named("check"), tasks.named("validateLinks"))
}

tasks.named("build") {
    dependsOn(tasks.named("buildStorybook"))
}

tasks.named("dockerCopyDistribution") {
    dependsOn(tasks.named("copyNginx"), tasks.named("copyBuildOutput"))
}

// Include some cloud-specific tasks only in the airbyte-platform-internal environment
if (file("${project.projectDir}/../../cloud/cloud-webapp/cloud-tasks.gradle").exists()) {
    apply(from = "${project.projectDir}/../../cloud/cloud-webapp/cloud-tasks.gradle")
}
