/*
 *  Licensed under the Apache License, Version 2.0 (the 'License');
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an 'AS IS' BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

plugins {
    base
}

tasks.register<Zip>("packageDistribution") {
    dependsOn(":rdf-delta-server:shadowJar")
    dependsOn(":rdf-delta-fuseki-server:shadowJar")
    archiveBaseName.set("rdf-delta")
    from("dist")
    from("Files")
    from("README")
    with(copySpec {
        from(tasks.getByPath(":rdf-delta-server:shadowJar"))
        rename({ name -> "delta-server.jar" })
    })
    with(copySpec {
        from(tasks.getByPath(":rdf-delta-fuseki-server:shadowJar"))
        rename({name -> "delta-fuseki.jar" })
    })
    with(copySpec {
        from(project.file("../rdf-delta-examples/Tutorial"))
        into("Tutorial")
    })
    into("rdf-delta-${project.version}")
}

