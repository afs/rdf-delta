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

package org.seaborne.delta.zk;

import org.apache.jena.atlas.json.JsonObject;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.function.Supplier;

public interface ZkConnection extends AutoCloseable {
    boolean pathExists(String path) throws Exception;

    String ensurePathExists(String path) throws Exception;

    byte[] fetch(String path) throws Exception;

    byte[] fetch(Watcher watcher, String path) throws Exception;

    JsonObject fetchJson(String path) throws Exception;

    JsonObject fetchJson(Watcher watcher, String path) throws Exception;

    List<String> fetchChildren(String path) throws Exception;

    List<String> fetchChildren(Watcher watcher, String path) throws Exception;

    String createZNode(String path) throws Exception;

    String createZNode(String path, CreateMode mode) throws Exception;

    String createAndSetZNode(String path, JsonObject object) throws Exception;

    String createAndSetZNode(String path, byte[] bytes) throws Exception;

    void setZNode(String path, JsonObject object) throws Exception;

    void setZNode(String path, byte[] bytes) throws Exception;

    void deleteZNodeAndChildren(String path) throws Exception;

    void runWithLock(String path, Runnable action);

    <X> X runWithLock(String path, Supplier<X> action);
}
