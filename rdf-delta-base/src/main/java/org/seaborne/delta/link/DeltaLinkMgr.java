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

package org.seaborne.delta.link;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.apache.jena.ext.com.google.common.collect.HashBiMap;
import org.seaborne.delta.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manage {@link DeltaLink} registrations */  
public class DeltaLinkMgr {
    private static Logger LOG = LoggerFactory.getLogger(DeltaLinkMgr.class);
    
    private BiMap<Id, RegToken> activeLinks = HashBiMap.create();
    private Map <UUID, RegToken> activeTokens = new ConcurrentHashMap<>();
    private Object syncObject = new Object();
    
    private static DeltaLinkMgr singleton = new DeltaLinkMgr();
    
    //public static DeltaLinkMgr get() { return singleton; }
    
    public DeltaLinkMgr() {}
    
    public RegToken register(Id clientId) {
        synchronized(syncObject) {
            if ( isRegistered(clientId) ) {
                LOG.warn("Registering client : "+clientId);
                // Existing registration - client restart?
                // Do a new registration.
                activeLinks.remove(clientId);
            }
            // New.
            RegToken token = new RegToken();
            activeLinks.put(clientId, token);
            activeTokens.put(token.getUUID(), token);
            return token;
        }
    }
    
    public boolean validate(Id tokenId) {
        synchronized(syncObject) {
            return activeLinks.containsKey(tokenId);
        }
    }
    
    public void deregister(RegToken token) {
        synchronized(syncObject) {
            if ( isRegistered(token) ) {
                activeLinks.inverse().remove(token);
            } else 
                LOG.warn("No such registration: token="+token);
        }
    }

    public void deregister(Id clientId) {
        synchronized(syncObject) {
            if ( isRegistered(clientId) ) {
                activeLinks.remove(clientId); 
            } else
                LOG.warn("No such registration: clientId="+clientId);
        }
    }

    public boolean isRegistered(Id clientId) {
        return activeLinks.containsKey(clientId);
    }

    public boolean isRegistered(RegToken regToken) {
        return activeLinks.containsValue(regToken);
    }
}
