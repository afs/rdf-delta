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

package org.seaborne.delta.server.local;

import org.apache.jena.atlas.logging.FmtLog;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An item under the control of a {@link LocalServer}.
 * <p>
 * These act as a record of the patch logs in a server, and are recorded in a {@link DataSourceRegistry}
 * so that routing by name or URI can be be done.
 */
public class DataSource {
    // Might be able to replace with "PatchLog".
    private static Logger LOG = LoggerFactory.getLogger(DataSource.class);
    private final DataSourceDescription dsDescription;
    private final PatchLog patchLog;

    public DataSource(DataSourceDescription dsd, PatchLog patchLog) {
        super();
        this.dsDescription = dsd;
        this.patchLog = patchLog;
        if ( ! dsd.equals(patchLog.getDescription()) )
            FmtLog.warn(LOG, "DSD %s not the same as in PatchLog %s", dsd, patchLog);
    }

    public Id getId() {
        return dsDescription.getId();
    }

    public String getURI() {
        return dsDescription.getUri();
    }

    public String getName() {
        return dsDescription.getName();
    }

    public PatchLog getPatchLog() {
        return patchLog;
    }

    public PatchStore getPatchStore() {
        return patchLog.getPatchStore();
    }

    public DataSourceDescription getDescription() {
        return dsDescription;
    }

    @Override
    public String toString() {
        return String.format("[DataSource:%s %s (%s)]",
                             dsDescription.getName(), dsDescription.getId(),
                             patchLog.getPatchStore().getProvider().getShortName());
    }
}
