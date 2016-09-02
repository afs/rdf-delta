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

package org.seaborne.delta.client;

import java.io.OutputStream ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFChangesWriter ;

public class DeltaConnection {

    /** Connect to a destination for changes */ 
    public static RDFChanges destination(String dest) {
        if ( dest.startsWith("file:") ) {
            OutputStream out = IO.openOutputFile(dest) ;
            RDFChanges sc = new RDFChangesWriter(out) ;
            return sc ;
        }
        
        if ( dest.startsWith("delta:") ) { // TCP connection delta:HOST:PORT
            throw new NotImplemented(dest) ; 
        }
        
        if ( dest.startsWith("http:") ) {
            // triggered on each transaction.
            return new RDFChangesHTTP(dest) ;
        }
        throw new IllegalArgumentException("Not understood: "+dest) ;
    }
}
