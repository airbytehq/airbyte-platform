plugins {
    id("io.airbyte.gradle.jvm")
    id("io.airbyte.gradle.docker")
    id("io.airbyte.gradle.publish")
}

airbyte {
    docker {
        imageName = "proxy"
    }
}

val prepareBuild = tasks.register<Copy>("prepareBuild") {
    from(layout.projectDirectory.file("nginx-auth.conf.template"))
    from(layout.projectDirectory.file("nginx-no-auth.conf.template"))
    from(layout.projectDirectory.file("run.sh"))
    from(layout.projectDirectory.file("401.html"))

    into(layout.buildDirectory.dir("airbyte/docker"))
}

tasks.named("dockerBuildImage") {
    dependsOn(prepareBuild)
    inputs.file("../.env")
}

val bashTest = tasks.register<Exec>("bashTest") {
    inputs.file(layout.projectDirectory.file("nginx-auth.conf.template"))
    inputs.file(layout.projectDirectory.file("nginx-no-auth.conf.template"))
    inputs.file(layout.projectDirectory.file("run.sh"))
    inputs.file(layout.projectDirectory.file("401.html"))
    outputs.upToDateWhen { true }
    dependsOn(tasks.named("dockerBuildImage"))
    commandLine("./test.sh")
}

// we can"t override the "test" command, so we can make our bash test a dependency)
tasks.named("test") {
    dependsOn(bashTest)
}
