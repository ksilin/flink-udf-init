/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java application project to get you started.
 * For more details on building Java & JVM projects, please refer to https://docs.gradle.org/8.12/userguide/building_java_projects.html in the Gradle documentation.
 */
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

version = "0.0.1"
group = "org.example"
base.archivesName.set("scalar-udf") // Controls the JAR filename

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {

    implementation(libs.bundles.flink)
    implementation(libs.bundles.log4j)
    testImplementation(libs.junit.jupiter)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

val shadowJar: ShadowJar by tasks
shadowJar.apply {
    //mergeServiceFiles()
    dependencies{
        exclude(dependency(".*:.*:.*"))
        //exclude(dependency("org.apache.flink:.*:.*"))
        // cannot handle the libs.bundle.logging or libs.slf4j notation
        //include(dependency("ch.qos.logback:logback-core:1.5.15"))
        //include(dependency("ch.qos.logback:logback-classic:1.5.15"))
        //include(dependency("org.slf4j:slf4j-api:2.0.16"))
    }
    archiveClassifier.set("shadow")
}

tasks {
    build {
        dependsOn(shadowJar)
    }
}

//application {
    // Define the main class for the application.
    // mainClass = "org.example.App"
//}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
