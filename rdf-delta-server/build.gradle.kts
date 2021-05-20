import com.github.jengelman.gradle.plugins.shadow.transformers.ApacheLicenseResourceTransformer
import com.github.jengelman.gradle.plugins.shadow.transformers.ApacheNoticeResourceTransformer

plugins {
    `jacoco`
    id("org.seaborne.rdf-delta.java-conventions")
    id("com.github.johnrengelman.shadow") version "7.0.0"
}

dependencies {
    implementation(project(":rdf-delta-server-http"))
    implementation(project(":rdf-delta-cmds"))
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${project.property("ver.log4j2")}")
}

description = "RDF Delta :: Delta server combined jar"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)

tasks.jacocoTestReport {
    reports {
        xml.isEnabled = true
        csv.isEnabled = false
        html.isEnabled = false
    }
}

tasks.shadowJar {
    mergeServiceFiles()
    transform(ApacheLicenseResourceTransformer::class.java)
    transform(ApacheNoticeResourceTransformer::class.java)
}
