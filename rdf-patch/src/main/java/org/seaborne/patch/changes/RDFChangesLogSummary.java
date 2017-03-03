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

package org.seaborne.patch.changes;

import org.apache.jena.ext.com.google.common.base.Objects;
import org.apache.jena.graph.Node;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.changes.RDFChangesLog.Printer;

public class RDFChangesLogSummary extends RDFChangesCounter {
    final private Printer printer;
    private Node node = null;
    private int depth = 0;

    public RDFChangesLogSummary() { this(RDFChangesLog::printer) ; }
    
    public RDFChangesLogSummary(Printer printer) {
        this.printer = printer ;
    }

    @Override
    public void start() { depth++ ; }
    
    @Override
    public void header(String field, Node value) {
        if ( Objects.equal(field, RDFPatch.ID) )
            node = value;
        super.header(field, value);
    }

    @Override
    public void finish() {
        depth-- ;
        if ( depth != 0 )
            return; 
        
        String s = "unset";
        if ( node != null ) {
            if ( node.isURI())
                s = node.getURI();
            else if ( node.isBlank() ) 
                s = node.getBlankNodeLabel();
            else
                s = node.getLiteralLexicalForm();
        }
        if ( s.length() > 11 )
            s = s.substring(0, 11)+"...";
        
        printer.print("%s :: QA: %d :: QD %d :: PA %d :: PD %d",
                      s,
                      countAddQuad,
                      countDeleteQuad,
                      countAddPrefix,
                      countDeletePrefix);
        super.reset();
    }
}

