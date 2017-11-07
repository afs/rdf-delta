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
        return getCommandName()+"--server URL";
    }
    
    @Override
    protected void execCmd() {
        execList();
    }

    protected void execList() {
        List <DataSourceDescription> all = getDescriptions();
        if ( all.isEmpty()) {
            System.out.println("-- No logs --");
            return ;
        }
        all.forEach(dsd->{
            PatchLogInfo logInfo = dLink.getPatchLogInfo(dsd.getId());
            if ( logInfo != null ) {
                System.out.print(
                                 String.format("[%s %s <%s> [%d,%d] %s]\n", dsd.getId(), dsd.getName(), dsd.getUri(), logInfo.getMinVersion(), logInfo.getMaxVersion(), 
                                               (logInfo.getLatestPatch()==null)?"<no patches>":logInfo.getLatestPatch().toString()));
            }
            else
                System.out.println(dsd);
        });
    }

    @Override
    protected void checkForMandatoryArgs() {}
}
