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

import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class OutputMgr {
    public static ManagedOutput create(String directoryName, String baseFilename, FilePolicy strategy) {
        Objects.requireNonNull(directoryName);
        Objects.requireNonNull(baseFilename);
        Objects.requireNonNull(strategy);
        return new OutputManagedFile(Paths.get(directoryName), baseFilename, strategy); 
    }
    
    public static ManagedOutput create(Path directory, String baseFilename, FilePolicy strategy) {
        Objects.requireNonNull(directory);
        Objects.requireNonNull(baseFilename);
        Objects.requireNonNull(strategy);
        return new OutputManagedFile(directory, baseFilename, strategy);
    }
    
    public static ManagedOutput create(String pathName, FilePolicy strategy) {
        Objects.requireNonNull(pathName);
        Objects.requireNonNull(strategy);
        Path p = Paths.get(pathName).toAbsolutePath();
        return new OutputManagedFile(p.getParent(), p.getFileName().toString(), strategy);
    }
    
    public static ManagedOutput create(OutputStream outputStream) {
        return new OutputFixed(outputStream); 
    }
}
