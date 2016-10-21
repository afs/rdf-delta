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

package org.seaborne.delta.pubsub;

import java.util.ArrayList ;
import java.util.List ;

import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesN ;
import org.seaborne.patch.changes.RDFChangesNoOp ;

/** A {@code Channel} is a sequence of change processors and a terminal action (guaranteed to the call last). 
 */
public class Channel {
    private static RDFChanges nothing = new RDFChangesNoOp() ;
    
    private        RDFChanges terminator = nothing ; 
    private List<RDFChanges>  handlers = new ArrayList<>() ;
    
    public static Channel.Builder create() { return new Channel.Builder() ; } 
    
    private Channel() {
        this(nothing) ;
    }
    
    private Channel(RDFChanges terminator) {
        this.terminator = terminator ;
    }
    
    public void publish(RDFPatch patch) {
        RDFChanges pipeline = new RDFChangesN(handlers) ;
        pipeline = RDFChangesN.multi(pipeline, terminator) ;
        patch.apply(pipeline);
    }
    
    public static class Builder {
        private Channel channel = new Channel() ;
        
        public Builder setTerminator(RDFChanges terminator) {
            channel.terminator = terminator ;
            return this ;
        }
        
        public Builder handler(RDFChanges handler) {
            channel.handlers.add(handler) ;
            return this ;
        }

        public Channel build() {
            Channel ch = channel ;
            channel = null ;
            return ch ;
        }
    }
    
}
