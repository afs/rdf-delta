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

package org.seaborne.delta.client;

import java.io.OutputStream ;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.text.RDFChangesWriterText;

public class DeltaClientLib {

    /** Connect to a destination for changes */
    public static RDFChanges destination(String dest) {
        // TODO text vs binary
        if ( dest.startsWith("file:") ) {
            OutputStream out = IO.openOutputFile(dest) ;
            RDFChanges sc = RDFChangesWriterText.create(out) ;
            return sc ;
        }

        if ( dest.startsWith("delta:") ) { // TCP connection delta:HOST:PORT
            throw new NotImplemented(dest) ;
        }

        if ( dest.startsWith("http:") || dest.startsWith("https:") ) {
            // Triggered on each transaction.
            return new RDFChangesHTTP(dest, dest) ;
        }
        throw new IllegalArgumentException("Not understood: "+dest) ;
    }

    /** A thread factory that creates {@link Thread#setDaemon daemon} threads. */
    public static ThreadFactory threadFactoryDaemon =
                  r -> {
                      Thread thread = Executors.defaultThreadFactory().newThread(r);
                      thread.setDaemon(true);
                      return thread;
                  };

//    Currently copied to DeltaLibHTTP.initialState
//    public static String makeURL(String url, String paramName1, Object paramValue1) {
//        return String.format("%s?%s=%s", url, paramName1, paramValue1);
//    }
//
//    public static String makeURL(String url, String paramName1, Object paramValue1, String paramName2, Object paramValue2) {
//        return String.format("%s?%s=%s&%s=%s", url, paramName1, paramValue1, paramName2, paramValue2);
//    }
//
//    public static String makeURL(String url, String paramName1, Object paramValue1, String paramName2, Object paramValue2, String paramName3, Object paramValue3) {
//        return String.format("%s?%s=%s&%s=%s&%s=%s", url, paramName1, paramValue1, paramName2, paramValue2, paramName3, paramValue3);
//    }
}
