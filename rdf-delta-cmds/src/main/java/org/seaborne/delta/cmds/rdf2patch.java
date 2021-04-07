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

package org.seaborne.delta.cmds;

import java.io.InputStream ;

import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.cmd.CmdException;
import org.apache.jena.cmd.CmdGeneral ;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF ;
import org.apache.jena.sys.JenaSystem;
import org.seaborne.patch.RDFPatchOps;

/** Write an RDF file as a patch file of "adds" (prefixes and triples/quads). */
public class rdf2patch extends CmdGeneral
{
    static {
        LogCtl.setLogging();
        JenaSystem.init();
    }

    public static void main(String... args) {
        new rdf2patch(args).mainRun();
    }

    public rdf2patch(String[] argv) {
        super(argv) ;
    }

    @Override
    protected String getSummary() {
        return "rdf2patch FILE" ;
    }

    @Override
    protected void processModulesAndArgs() {
        super.processModulesAndArgs();
        if ( super.positionals.isEmpty() )
            throw new CmdException("File argument required. Usage: "+getSummary());
    }

    @Override
    protected void exec() {
        StreamRDF dest  = RDFPatchOps.write(System.out);
        dest.start();
        if ( getPositional().isEmpty() )
            execOne(System.in);
        getPositional().forEach(fn->RDFParser.source(fn).parse(dest));
        dest.finish();
    }

    private void execOne(InputStream input) {
        throw new CmdException("Reading from stdin not implemented");
    }

    @Override
    protected String getCommandName() {
        return null ;
    }

}
