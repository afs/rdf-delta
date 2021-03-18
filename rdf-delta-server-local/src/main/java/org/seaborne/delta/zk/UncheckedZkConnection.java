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

public interface UncheckedZkConnection extends ZkConnection {
    @Override
    boolean pathExists(String path);

    @Override
    String ensurePathExists(String path);

    @Override
    byte[] fetch(String path);

    @Override
    byte[] fetch(Watcher watcher, String path);

    @Override
    JsonObject fetchJson(String path);

    @Override
    JsonObject fetchJson(Watcher watcher, String path);

    @Override
    List<String> fetchChildren(String path);

    @Override
    List<String> fetchChildren(Watcher watcher, String path);

    @Override
    String createZNode(String path);

    @Override
    String createZNode(String path, CreateMode mode);

    @Override
    String createAndSetZNode(String path, JsonObject object);

    @Override
    String createAndSetZNode(String path, byte[] bytes);

    @Override
    void setZNode(String path, JsonObject object);

    @Override
    void setZNode(String path, byte[] bytes);

    @Override
    void deleteZNodeAndChildren(String path);

    @Override
    void close();
}
