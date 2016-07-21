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

package org.seaborne.delta;

import java.io.InputStream ;
import java.io.OutputStream ;

import org.apache.commons.lang3.NotImplementedException ;
import org.apache.jena.atlas.io.IO ;
import org.apache.jena.shared.JenaException ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.DP.DatasetGraphChangesVersion ;
import org.seaborne.delta.base.PatchReader ;
import org.seaborne.delta.changes.StreamChanges ;
import org.seaborne.delta.changes.StreamChangesApply ;
import org.seaborne.delta.changes.StreamChangesWriter ;
import org.seaborne.delta.client.LibPatchSender ;
import org.seaborne.delta.client.StreamChangesCollect ;

public class DeltaOps {
    
    public static StreamChanges connect(String dest) {
        
        if ( dest.startsWith("file:") ) {
            OutputStream out = IO.openOutputFile(dest) ;
            StreamChanges sc = new StreamChangesWriter(out) ;
            return sc ;
        }
        
        if ( dest.startsWith("delta:") ) { // TCP connection delta:HOST:PORT
            throw new NotImplementedException(dest) ; 
        }
        
        if ( dest.startsWith("http:") ) {
            // triggered on each transaction.
            return new StreamChangesCollect(dest) ;
        }
        throw new IllegalArgumentException("Not understood: "+dest) ;
    }

    public static DatasetGraph managedDatasetGraph(DatasetGraph dsg, String url) {
        StreamChangesCollect changes = LibPatchSender.create1(url) ;
        DatasetGraph dsg1 = new DatasetGraphChangesVersion(dsg, changes);
        return dsg1 ;
    }
    
    private static OutputStream openChangesDest(String x) {
        if ( x.startsWith("file:") )
            return IO.openOutputFile(x) ;
        if ( x.startsWith("delta:") ) { // delta:HOST:PORT
            throw new NotImplementedException(x) ; 
        }
        throw new JenaException("Not understood: "+x) ;
    }
    
    public static void play(DatasetGraph dsg, InputStream input) {
        PatchReader pr = new PatchReader(input) ;
        StreamChanges sc = new StreamChangesApply(dsg) ; 
        pr.apply(sc);
    }
}
