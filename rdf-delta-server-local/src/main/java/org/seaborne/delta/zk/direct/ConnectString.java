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

package org.seaborne.delta.zk.direct;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * <p>
 * Parses a config to create a connectString.
 * </p>
 *
 * <p>
 * The following ZooKeeper Ensemble config
 * </p>
 *
 * <blockquote>
 * server.1=localhost:2182:2183:participant;0.0.0.0:2181<br/>
 * server.2=localhost:2282:2283:participant;0.0.0.0:2281<br/>
 * server.3=localhost:2382:2383:participant;0.0.0.0:2381<br/>
 * version=700000008
 * </blockquote>
 *
 * <p>
 * would be parsed into the following connectString:
 * </p>
 *
 * <blockquote>
 * localhost:2181,localhost:2281,localhost:2381
 * </blockquote>
 */
public final class ConnectString implements CharSequence {
    /**
     * The parsed connectString.
     */
    private final String connectString;

    /**
     * Constructs a new {@link ConnectString} from an Ensemble Config retrieved from /zookeeper/config.
     * @param config The value at /zookeeper/config.
     */
    public ConnectString(final byte[] config) {
        if (config.length == 0) {
            this.connectString = "";
        } else {
            this.connectString = Arrays.stream(new String(config).split("\n"))
                .filter(s -> s.startsWith("server"))
                .map(s -> s.split("=")[1])
                .map(
                    s -> {
                        var elements = s.split(":");
                        return String.format("%s:%s", elements[0], elements[elements.length - 1]);
                    }
                ).collect(Collectors.joining(","));
        }
    }

    @Override
    public int length() {
        return this.connectString.length();
    }

    @Override
    public char charAt(int index) {
        return this.connectString.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return this.connectString.subSequence(start, end);
    }

    @Override
    public String toString() {
        return this.connectString;
    }
}
