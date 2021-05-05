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

/**
 * A version of {@link ZkConnection} that does not throw checked exceptions.
 *
 * <p>
 *     This is introduced for compatibility with the old setup. Ideally, exception handling should be deferred to the
 *     highest level of interest rather than logging and discarding exceptions and returning {@code null}.
 * </p>
 */
public interface UncheckedZkConnection extends ZkConnection {
    @Override
    public boolean pathExists(String path);

    @Override
    public String ensurePathExists(String path);

    @Override
    public byte[] fetch(String path);

    @Override
    public byte[] fetch(Watcher watcher, String path);

    @Override
    public JsonObject fetchJson(String path);

    @Override
    public JsonObject fetchJson(Watcher watcher, String path);

    @Override
    public List<String> fetchChildren(String path);

    @Override
    public List<String> fetchChildren(Watcher watcher, String path);

    @Override
    public String createZNode(String path);

    @Override
    public String createZNode(String path, CreateMode mode);

    @Override
    public String createAndSetZNode(String path, JsonObject object);

    @Override
    public String createAndSetZNode(String path, byte[] bytes);

    @Override
    public void setZNode(String path, JsonObject object);

    @Override
    public void setZNode(String path, byte[] bytes);

    @Override
    public void deleteZNodeAndChildren(String path);

    @Override
    public void close();
}
