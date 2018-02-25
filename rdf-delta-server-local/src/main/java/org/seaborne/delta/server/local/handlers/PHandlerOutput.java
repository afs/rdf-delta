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

package org.seaborne.delta.server.local.handlers;

import java.io.OutputStream ;

import org.seaborne.delta.DeltaOps ;
import org.seaborne.delta.server.local.Patch;
import org.seaborne.delta.server.local.PatchHandler;
import org.seaborne.patch.changes.RDFChangesWriter ;
import org.seaborne.patch.text.TokenWriter ;

/** Write a patch to an {@code OutputStream}. */
public class PHandlerOutput implements PatchHandler {
    
    private final RDFChangesWriter scWriter ;
    
    public PHandlerOutput(OutputStream output) {
        TokenWriter tokenWriter = DeltaOps.tokenWriter(output) ;
        scWriter = new RDFChangesWriter(tokenWriter) ;
    }
    
    @Override
    synchronized // XXX revise
    public void handle(Patch patch) {
        scWriter.start() ;
        patch.play(scWriter);
        scWriter.finish() ;
    }
}
