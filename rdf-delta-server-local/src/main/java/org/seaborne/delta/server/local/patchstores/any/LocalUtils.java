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

package org.seaborne.delta.server.local.patchstores.any;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.seaborne.delta.server.local.PatchLog;

/** Operations on "local" patch store - ones stored in the file system */
public class LocalUtils {

    public static void movePatchLog(PatchLog log, Path src, Path dst) {



        src = src.toAbsolutePath();
        dst = dst.toAbsolutePath();

        // close open.

        if ( ! Files.exists(src) ) {}
        if ( Files.exists(dst) ) {}

        try {
            Files.move(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // fix up source.cfg.

        // Open new.

    }

}
