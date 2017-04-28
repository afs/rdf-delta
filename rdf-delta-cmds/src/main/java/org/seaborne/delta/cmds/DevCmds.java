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

public class DevCmds extends DeltaCmd {
    public static void main(String ... argv) {
        //list.main("--server=http://localhost:1066/");
        mksrc.main("--server=http://localhost:1066/", "--dsrc=ABC");
        System.exit(0);
        
        new DevCmds(argv).mainRun();
        
    }

    public DevCmds(String[] argv) {
        super(argv) ;
    }

    @Override
    protected void execCmd() {
        ping();

        list();
        System.out.println();
        
        if ( true ) return ;
        
        String name = "ABC";
        String s = serverURL;
        if ( ! s.endsWith("/") )
            s = s+"/";
        String url = s+name;
        create("ABC", url);
        System.out.println();
        
        hide(name);
        System.out.println();

        list();
        System.out.println();


    }

    @Override
    protected String getSummary() {
        return null ;
    }

    @Override
    protected void checkForMandatoryArgs() {}
}
