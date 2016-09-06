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

import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.base.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFChangesApply ;
import org.seaborne.patch.RDFChangesLog ;
import org.seaborne.patch.RDFChangesN ;

public class DeltaOps {
    
//    public static DatasetGraph managedDatasetGraph(DatasetGraph dsg, String url) {
//        RDFChangesHTTP changes = LibPatchSender.create1(url) ;
//        DatasetGraph dsg1 = new DatasetGraphChangesVersion(dsg, changes);
//        return dsg1 ;
//    }
//    
    
    /** Called closes the {@link InputStream}. */
    public static void play(DatasetGraph dsg, InputStream input) {
        PatchReader pr = new PatchReader(input) ;
        RDFChanges sc = new RDFChangesApply(dsg) ; 
        pr.apply(sc);
    }
    
    /** Add a printer to a {@link RDFChanges} */
    public static RDFChanges print(RDFChanges changes) {
        return RDFChangesN.multi(changes, new RDFChangesLog()) ;
    }

}
