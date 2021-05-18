/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.seaborne.rdf-delta.java-conventions")
}

dependencies {
    implementation(project(":rdf-patch"))
    implementation(project(":rdf-delta-base"))
    implementation(project(":rdf-delta-fuseki"))
    implementation(project(":rdf-delta-client"))
    implementation("org.apache.jena:jena-fuseki-main:3.17.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.1")
    implementation("org.apache.jena:jena-text:3.17.0")
}

description = "RDF Delta :: Delta + Fuseki"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
