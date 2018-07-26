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

import java.io.IOException ;
import java.io.InputStream ;
import java.nio.file.Files ;
import java.nio.file.Path ;
import java.nio.file.Paths ;

import jena.cmd.CmdException ;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.StreamRDF;
import org.seaborne.delta.Delta;
import org.seaborne.delta.Id ;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.Version;
import org.seaborne.delta.lib.IOX;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.RDF2Patch;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

/** Create a new log */
public class append extends DeltaCmd {
    
    public static void main(String... args) {
        new append(args).mainRun();
    }

    public append(String[] argv) {
        super(argv) ;
        super.add(argLogName);
        super.add(argDataSourceURI);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server URL --log NAME PATCH ...";
    }
    
    @Override
    protected void execCmd() {
        LogCtl.disable(Delta.DELTA_HTTP_LOG.getName());
        getPositional().forEach(fn->exec1(fn));
    }
    
    protected void exec1(String fn) {
        Id dsRef = getDescription().getId();
        PatchLogInfo info = dLink.getPatchLogInfo(dsRef);
        Id prev = info.getLatestPatch();
        RDFPatch body = toPatch(fn);

        // Header.
        PatchHeader header = RDFPatchOps.makeHeader(Id.create().asNode(), prev==null?null:prev.asNode());
        RDFPatch patch = RDFPatchOps.withHeader(header, body);
        //RDFPatchOps.write(System.out, patch);
        // Add previous.
        Version version = dLink.append(dsRef, patch);
        System.out.printf("Version = %s\n", version.value());
    }

    protected RDFPatch toPatch(String fn) {
        // .gz??
        Lang lang = RDFLanguages.filenameToLang(fn);
        if ( lang != null && ( RDFLanguages.isTriples(lang) || RDFLanguages.isQuads(lang) ) ) {
            RDFChangesCollector x = new RDFChangesCollector();
            StreamRDF dest  = new RDF2Patch(x);
            // dest will do the start-finish on the RDFChangesCollector via parsing.
            RDFDataMgr.parse(dest, fn);
            return x.getRDFPatch();
        }
        
        // Not RDF - assume a text patch.
//        String ext = FileOps.extension(fn);
//        switch(ext) {
//            case RDFPatchConst.EXT:
//                break;
//            case RDFPatchConst.EXT_B:
//                break;
//            default:
//                Log.warn(addpatch.class, "Conventionally, patches have file extension ."+RDFPatchConst.EXT);
//        }
        
        Path path = Paths.get(fn);
        try(InputStream in = Files.newInputStream(path) ) {
            return RDFPatchOps.read(in);
        } catch (IOException ex ) { throw IOX.exception(ex); }
    }
    
    @Override
    protected void checkForMandatoryArgs() {
        if ( !contains(argLogName) && ! contains(argDataSourceURI) ) 
            throw new CmdException("Required: one of --"+argLogName.getKeyName()+" or --"+argDataSourceURI.getKeyName());
        if ( getPositional().isEmpty() ) {
            throw new CmdException(getCommandName()+" : No patch files"); 
        }
    }
}
