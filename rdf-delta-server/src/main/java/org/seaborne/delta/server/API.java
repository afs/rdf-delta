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

package org.seaborne.delta.server;

import java.io.InputStream ;
import java.util.Iterator ;

import org.seaborne.delta.pubsub.Distributor ;
import org.seaborne.delta.pubsub.Receiver ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;
import org.seaborne.patch.changes.RDFChangesCollector ;

public class API {
    
    static Distributor distributor = new Distributor() ;
    
    static {
        // Setup
    }
    
    public static class RegToken {
        
    }
    
    public static class Registration {
        RegToken regToken ;
        // Graphs, datasets? Channels?
        // new stuff?? 
    }
    
    public static Registration register(Id clientId) {
        distributor.register(null, null) ;
        return null ;
    }
    
    public static Registration register(String name) {
        return null ;
    }

    public Registration register(String name, Id id) {
        return null ;
    }
    
    public static void deregister(RegToken token) {}

    public static void receive(Id ref, InputStream in) {
        DataSource source = DataRegistry.get().get(ref) ;
        // id -> registation
        RDFPatch patch = consume(source, in) ;
        System.out.println("Patch:") ;
        RDFPatchOps.write(System.out, patch) ;
    }
    
    
    /** Process an {@code InputStream} and return an RDFPatch */
    private static RDFPatch consume(DataSource source, InputStream in) {
        // XXX Switch to a spilling collector.
        RDFChangesCollector collector = new RDFChangesCollector() ;
        
        // XXX source .locationOfPatchStorage .
        
        Receiver receiver = new Receiver() ;
        receiver.receive(in, collector);
        return collector.getRDFPatch() ;
    }
    
//    public static InChannel getChannel(Id data) {
//        DataRef ref = getDataRef(data) ;
//        if ( ref == null )
//            return null ;
//        return ref.channel() ;
//    }
    
    public static DataRef getDataRef(Id data) {
        return  Datasets.get(data) ;
    }
    
    public static PatchSetInfo info(ChannelName channel) {
        return null ;
    }
    
    
    public static Iterator<Patch> patches(Id start, Id finish) {
        return null ;
    }

    // Dataset system
    
    public void existingDataset() {} 
    
    public Id newDataset() { return null ; }
    public void deleteDataset(Id uuid) { }

    // Graph-only system
    
    public Id newGraph(String uri) { return null ; }

    
    // New graph(base Id)
    
    
    
}
