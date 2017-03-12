/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.delta.cmds;

import java.io.InputStream ;

import jena.cmd.CmdGeneral ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.system.StreamRDF ;
import org.seaborne.patch.StreamPatch ;

/** Write an RDF file to a patch file of adds */
public class rdf2patch extends CmdGeneral
{
    static { LogCtl.setCmdLogging() ; }
    
    public static void main(String[] args) {
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
    }
    
    // System.in not fworkign yet.
    // Extend "riot"?
    
    @Override
    protected void exec() {
        StreamRDF dest  = new StreamPatch(System.out);
        dest.start();
        
        if ( getPositional().isEmpty() )
            execOne(System.in);
        getPositional().forEach(fn->{
            RDFDataMgr.parse(dest, fn);
//            InputStream input = IO.openFile(fn);
//            execOne(input);
        });

        dest.finish();
    }
    

    private void execOne(InputStream input) {
        
        
    }
    
    @Override
    protected String getCommandName() {
        return null ;
    }
    
}
