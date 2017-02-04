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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.graph.Node;
import org.seaborne.delta.Id;
import org.seaborne.patch.RDFPatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Server-wide cache of patches */ 
public class PatchCache {
    private static Logger  LOG     = LoggerFactory.getLogger(PatchCache.class);
    
//    private Cache<Node, RDFPatch> patchCache;
//    private PatchCache() { 
//        patchCache = CacheFactory.createCache(DPConst.PATCH_CACHE_SIZE);
//        patchCache.setDropHandler((node,patch)->LOG.info("Cache drop patch: "+Id.fromNode(node)));
//    }
    
    private ConcurrentHashMap<Node, RDFPatch> patchCache;
    private PatchCache() { 
        patchCache = new ConcurrentHashMap<>();
        //patchCache.setDropHandler((node,patch)->LOG.info("Cache drop patch: "+Id.fromNode(node)));
    }
    private static PatchCache singleton = new PatchCache();
    
    public static  PatchCache get() { return singleton ; } 
    
    public RDFPatch get(Id id) {
        //return patchCache.getIfPresent(id.asNode());
        return patchCache.get(id.asNode());
    }

    public void put(Id id, RDFPatch patch) {
        put(id.asNode(), patch);
    }

    public void put(Node node, RDFPatch patch) {
        patchCache.put(node, patch);
    }
}
