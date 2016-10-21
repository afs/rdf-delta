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

import java.util.HashMap ;
import java.util.Map ;

import org.apache.jena.ext.com.google.common.collect.Multimap ;

public class Distributor {
    private Map<String, Channel> inbound = new HashMap<>() ;
    private Multimap<Key, Channel> registrations = null ;  
    
    /** Register:
     *  
     * @param client
     * @param channelName name
     */
    
    public Channel register(String client, String channelName) {
        
        
        Channel channel = Channel.create().build() ;
        return null ;
    }
    
    public Channel getChannel(Object key) {
        return null ;
    }
}
