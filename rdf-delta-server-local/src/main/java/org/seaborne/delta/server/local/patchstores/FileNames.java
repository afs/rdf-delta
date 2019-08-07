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

package org.seaborne.delta.server.local.patchstores;

/** File names for the file-based patch store provider. */
public class FileNames {
    /** Name for the DataSource configuration file for the file-based provider. */
    public static final String DS_CONFIG       = "source.cfg";

    /** Relative path name in a DataSource for the "sources" area. */
    public static final String SOURCES         = "Sources";

    /** Relative path name in a DataSource for the log area. */
    public static final String LOG             = "Log";

    /** Marker file for "deletes" data sources (they are only hidden) */
    public static final String DISABLED        = "disabled";
}
