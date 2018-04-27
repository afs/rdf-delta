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

package org.seaborne.patch.rotate;

import java.nio.file.Path;

/** {@link Roller} that is a fixed file. */
class RollerFixed implements Roller {
    private final Path directory;
    private final String baseFilename;
    private String filename = null; 
    

    RollerFixed(Path directory, String baseFilename) {
        this.directory = directory;
        this.baseFilename = baseFilename;
        this.filename = directory.resolve(baseFilename).toString();
    }

    @Override
    public boolean hasExpired() {
        return false;
    }

    @Override
    public void forceRollover() {}

    @Override
    public String nextFilename() {
        return filename;
    }
}