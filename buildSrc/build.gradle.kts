plugins {
    id("groovy-gradle-plugin")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("ru.vyarus:gradle-use-python-plugin:2.3.0")
}

tasks.withType<Jar>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
