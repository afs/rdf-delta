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
import org.seaborne.patch.changes.RDFChangesWriter ;
import org.seaborne.patch.text.TokenWriter ;
import org.seaborne.patch.text.TokenWriterText ;

public class DeltaLib {
    
    // XXX More here?
    // Or less? (only use by the assembler?)
    // See also DeltaClient. Assembler to use DeltaClient?

    /** Connect to a destination for changes */
    public static RDFChanges destination(String dest) {
        // TODO text vs binary
        if ( dest.startsWith("file:") ) {
            OutputStream out = IO.openOutputFile(dest) ;
            TokenWriter tokenWriter = new TokenWriterText(out) ;
            RDFChanges sc = new RDFChangesWriter(tokenWriter) ;
            return sc ;
        }
        
        if ( dest.startsWith("delta:") ) { // TCP connection delta:HOST:PORT
            throw new NotImplemented(dest) ; 
        }
        
        if ( dest.startsWith("http:") || dest.startsWith("https:") ) {
            // Triggered on each transaction.
            return new RDFChangesHTTP(dest, ()->dest, null) ;
        }
        throw new IllegalArgumentException("Not understood: "+dest) ;
    }
    
    public static String makeURL(String url, String paramName1, Object paramValue1) {
        return String.format("%s?%s=%s", url, paramName1, paramValue1);
    }
    
    public static String makeURL(String url, String paramName1, Object paramValue1, String paramName2, Object paramValue2) {
        return String.format("%s?%s=%s&%s=%s", url, paramName1, paramValue1, paramName2, paramValue2);
    }
    
    public static String makeURL(String url, String paramName1, Object paramValue1, String paramName2, Object paramValue2, String paramName3, Object paramValue3) {
        return String.format("%s?%s=%s&%s=%s&%s=%s", url, paramName1, paramValue1, paramName2, paramValue2, paramName3, paramValue3);
    }
}
