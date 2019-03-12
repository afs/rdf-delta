/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.fuseki.cmd;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServlet;

import jena.cmd.ArgDecl;
import jena.cmd.CmdException;
import jena.cmd.CmdGeneral;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.main.JettyServer;
import org.seaborne.delta.Delta;
import org.seaborne.delta.fuseki.PatchWriteServlet;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.patch.filelog.FilePolicy;
import  org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaBackupServer {

    private static Logger LOG = LoggerFactory.getLogger("Backup");

    public static void main(String... args) {
        // Stop Fuseki trying to initialize logging using log4j.
        System.setProperty("log4j.configuration", "delta");
        LogCtl.setJavaLogging();
        new Inner(args).mainRun();
    }

    public static JettyServer build(String...args) {
        Delta.init();
        Inner inner = new Inner(args);
        inner.process() ;
        return inner.build() ;
    }

    static class BackupArea {
        final String name;
        final String dir;
        final String file;
        public BackupArea(String httpName, String dir, String file) {
            this.name = httpName;
            this.dir = dir;
            this.file = file;
        }
    }

    static class BackupConfig {
        int port;
        List<BackupArea> logs =  new ArrayList<>();
    }

    static class Inner extends CmdGeneral {
        private final ArgDecl argPort     = new ArgDecl(true, "port");
        private final ArgDecl argConf     = new ArgDecl(true, "config", "cfg");
        private final ArgDecl argDir      = new ArgDecl(true, "dir");
        private final ArgDecl argFile     = new ArgDecl(true, "file");
        private final ArgDecl argName     = new ArgDecl(true, "name");

        protected Inner(String[] argv) {
            super(argv);
            super.add(argConf);
            super.add(argDir);
            super.add(argFile);
            super.add(argName);
            super.add(argPort);
        }

        @Override
        protected String getSummary() {
            return null;
        }

        @Override
        protected String getCommandName() {
            return DeltaBackupServer.class.getSimpleName();
        }

        @Override
        protected void exec() {
            JettyServer server = build();
            try {
                server.start();
            }
            catch (Exception ex) {
                throw new CmdException("Failed to start: "+ex.getMessage(), ex);
            }
            server.join();
        }

        /**
         * Build a web server - a Fuseki server with no datasets - it will then support
         * general Fuseki servlets.
         */
        protected JettyServer build() {
            BackupConfig cfg = new BackupConfig();

            int port = 1096;
            String portStr = getValue(argPort);
            if ( portStr != null ) {
                try {
                    port = Integer.parseInt(portStr);
                    if ( port <= 0 && port >= 64*1024 )
                        throw new CmdException("Bad port number: "+portStr);
                }
                catch (NumberFormatException ex) {
                    throw new CmdException("Failed to parse port number: "+portStr);
                }
            }
            cfg.port = port;

            if ( contains(argConf) ) {
                if ( ! getValue(argName).isEmpty() || ! getValues(argDir).isEmpty() || getValues(argFile).isEmpty() ) {
                    throw new CmdException("Can't have command line specified log area and also a configuration file");
                }
                parseConf(cfg, getValue(argConf));
            } else {
                // no --conf
                if ( getValues(argName).isEmpty() || getValues(argDir).isEmpty() || getValues(argFile).isEmpty() )
                    throw new CmdException("Must provide either a configuration file with --conf or provide --name, --dir and --file arguments");
                if ( getValues(argName).size() != 1 || getValues(argDir).size() != 1 || getValues(argFile).size() != 1 )
                    throw new CmdException("Must have exactly one each of --name, --dir and --file");

                String name = getValue(argName);
                String dir = getValue(argDir);
                String file = getValue(argFile);
                cfg.logs.add(new BackupArea(name, dir, file));
            }

            //writeConf(cfg);

            JettyServer.Builder builder = JettyServer.create().port(cfg.port).verbose(isVerbose());
            cfg.logs.forEach(a->{
                // More Path-ness
                LOG.info(format("Backup area: (area=%s, dir='%s', file='%s')", a.name, a.dir, a.file));
                HttpServlet handler = new PatchWriteServlet(a.dir, a.file, FilePolicy.INDEX);
                // XXX Make better
                String x = a.name;
                if ( ! a.name.startsWith("/") )
                    x = "/"+a.name;
                builder.addServlet(x, handler);
            });
            JettyServer server = builder.build();
            return server;
        }

        // Configuration I/O.

        /*
         * {
         *    "logs": [
         *       { "name", "HTTP name", "dir": "DIR", "file": "FILE" }
         *    ]
         * }
         */

        private static final String jPort = "port";
        private static final String jLogs = "logs";
        private static final String jName = "names";
        private static final String jDir  = "dir";
        private static final String jFile = "file";

        private void parseConf(BackupConfig cfg, String cfgFile) {
            try {
                JsonObject obj = JSON.read(cfgFile);
                cfg.port = obj.get(jPort).getAsNumber().value().intValue();
                JsonArray a = obj.get(jLogs).getAsArray();
                a.forEach(elt-> {
                    BackupArea area = parseLogObject(cfg, elt);
                    cfg.logs.add(area);
                });
            } catch (Exception ex) {
                throw new CmdException("Failed to process configuration file: "+ex.getMessage());
            }
        }

        private BackupArea parseLogObject(BackupConfig cfg, JsonValue elt) {
            String name = elt.getAsObject().get(jName).getAsString().value();
            String dir = elt.getAsObject().get(jDir).getAsString().value();
            String file = elt.getAsObject().get(jFile).getAsString().value();
            if ( name == null || dir == null || file == null )
                throw new CmdException("Required: \""+jName+", \""+jDir+"\" and \""+jFile+"\"");
            return new BackupArea(name, dir, file);
        }

        private void writeConf(BackupConfig cfg) {
            JsonObject obj = JSONX.buildObject(b->{
                b   .pair(jPort, cfg.port)
                    .key(jLogs).startArray();
                cfg.logs.forEach(a->
                    b.startObject().pair(jName, a.name).pair(jDir, a.dir).pair(jFile, a.file).finishObject()
                    );
                b.finishArray();
            });
            JSON.write(System.out, obj);
        }
   }
}
