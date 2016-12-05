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

package org.seaborne.delta.server.local;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.apache.jena.ext.com.google.common.collect.HashBiMap;

//import java.util.Map;

import org.seaborne.delta.conn.Id;
import org.seaborne.delta.conn.RegToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manage DeltaConnections to this Server */  
public class DeltaConnectionMgr {
    private static Logger LOG = LoggerFactory.getLogger(DeltaConnectionMgr.class);
    
    private BiMap<Id, RegToken> activeClients = HashBiMap.create();
    private Map <UUID, RegToken> activeTokens = new ConcurrentHashMap<>();
    private Object syncObject = new Object();
    private static DeltaConnectionMgr singleton = new DeltaConnectionMgr();
    public static DeltaConnectionMgr get() { return singleton; }
    
    public RegToken register(Id clientId) {
        synchronized(syncObject) {
            if ( activeClients.containsKey(clientId) ) {
                // Existing registration - cliebnt restart?
                // Do a new registration.
                activeClients.remove(clientId);
            }
         
            // New.
            RegToken token = new RegToken();
            activeClients.put(clientId, token);
            activeTokens.put(token.getUUID(), token);
            
            return token;
        }
    }
    
    public boolean validate(Id tokenId) {
        synchronized(syncObject) {
            return activeClients.containsKey(tokenId);
        }
    }
    
    public void deregister(RegToken token) {
        synchronized(syncObject) {
            if ( activeClients.containsValue(token) ) {
                // Existing registration - cliebnt restart?
                // Do a new registration.
                activeClients.inverse().remove(token);
            } else 
                LOG.warn("No such registration: token="+token);
        }
    }

    public void deregister(Id clientId) {
        synchronized(syncObject) {
            if ( activeClients.containsKey(clientId) ) {
                // Existing registration - cliebnt restart?
                // Do a new registration.
                activeClients.remove(clientId); 
            } else
                LOG.warn("No such registration: clientId="+clientId);
        }
    }

    
}
