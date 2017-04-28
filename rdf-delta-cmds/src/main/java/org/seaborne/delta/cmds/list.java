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

import org.apache.jena.atlas.logging.LogCtl ;

/** Create a new log */
public class list extends DeltaCmd {

static { LogCtl.setCmdLogging() ; }
    
    public static void main(String... args) {
        new list(args).mainRun();
    }

    public list(String[] argv) {
        super(argv) ;
        super.add(argDataSourceName);
        super.add(argDataSourceURL);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+"--server URL";
    }
    
    @Override
    protected void execCmd() {
        list();
    }

    @Override
    protected void checkForMandatoryArgs() {}
}
