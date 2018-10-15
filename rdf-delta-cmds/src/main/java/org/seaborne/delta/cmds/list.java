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

package org.seaborne.delta.cmds;

import java.util.List ;

import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.PatchLogInfo ;
import org.seaborne.delta.Version;

/** Create a new log */
public class list extends DeltaCmd {

    public static void main(String... args) {
        new list(args).mainRun();
    }

    public list(String[] argv) {
        super(argv) ;
        super.add(argLogName);
        super.add(argDataSourceURI);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server URL [--log=NAME]";
    }

    @Override
    protected void execCmd() {
        if ( dataSourceName != null ) {
            execOneName(dataSourceName);
        }
        else if ( super.dataSourceURI != null ) {}
        else {
        execList();
        }
    }

    private void execOneName(String name) {
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByName(name);
        detailsByDSD(dsd);
    }

    private void execOneURI(String uriStr) {
        DataSourceDescription dsd = dLink.getDataSourceDescriptionByURI(uriStr);
        detailsByDSD(dsd);
    }

    protected void execList() {
        List <DataSourceDescription> all = getDescriptions();
        if ( all.isEmpty()) {
            System.out.println("-- No logs --");
            return ;
        }
        all.forEach(this::detailsByDSD);
    }

    private void detailsByDSD(DataSourceDescription dsd) {
        PatchLogInfo logInfo = dLink.getPatchLogInfo(dsd.getId());
        if ( logInfo == null ) {
            // Some thing bad somewhere.
            System.out.printf("[%s %s <%s> [no info] %s]\n", dsd.getId(), dsd.getName(), dsd.getUri());
            return;
        }
        if ( Version.INIT.equals(logInfo.getMinVersion()) && Version.INIT.equals(logInfo.getMaxVersion()) ) {
            if ( logInfo.getLatestPatch() != null )
                // Should not happen.
                System.out.printf("[%s %s <%s> [empty] %s]\n", dsd.getId(), dsd.getName(), dsd.getUri(), logInfo.getLatestPatch().toString());
            else
                System.out.printf("[%s %s <%s> [empty]]\n", dsd.getId(), dsd.getName(), dsd.getUri());
            return;
        }
        if ( logInfo.getMinVersion().isValid() ) {
            System.out.printf("[%s %s <%s> [%s,%s] %s]\n", dsd.getId(), dsd.getName(), dsd.getUri(),
                logInfo.getMinVersion(), logInfo.getMaxVersion(),
                (logInfo.getLatestPatch()==null)?"<no patches>":logInfo.getLatestPatch().toString()
                );
        }

    }

    @Override
    protected void checkForMandatoryArgs() {}
}
