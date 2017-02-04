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

import org.apache.jena.atlas.lib.Cache;
import org.apache.jena.atlas.lib.CacheFactory;
import org.apache.jena.graph.Node;
import org.seaborne.delta.DPConst;
import org.seaborne.delta.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Server-wide cache of patches */ 
public class PatchCache {
    private static Logger  LOG     = LoggerFactory.getLogger(PatchCache.class);
    
    // Global id->patch cache.
    private static Cache<Node, Patch> patchCache = CacheFactory.createCache(DPConst.PATCH_CACHE_SIZE);
    static {
        patchCache.setDropHandler((node,patch)->LOG.info("Cache drop patch: "+Id.fromNode(node)));
    }
}
