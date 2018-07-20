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

package delta.server;

import java.io.PrintStream;

import org.apache.jena.atlas.io.NullOutputStream;
import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.fuseki.FusekiLib;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.PatchLogInfo;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.cmds.dcmd;
import org.seaborne.delta.lib.LogCtlX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLog;

public class DS {

    public static void main(String...args) {
        DeltaServer.setLogging();
        // change our mind
        LogCtlX.setJavaLoggingClasspath("logging-quiet.properties");
        
        try {
            exec();
        } finally { System.exit(0); }
    }

    static PrintStream devnull= new PrintStream(new NullOutputStream());
    
    static void execNoOutput(Runnable action) {
        PrintStream x = System.out;
        try {
            System.setOut(devnull);
            action.run();
        } finally { System.setOut(x); }
    }
    
    public static void exec() {
    
        //Clean.
        
        String DIR = "/home/afs/tmp/ZKD";
        FileOps.clearDirectory(DIR);
        
        int PORT = FusekiLib.choosePort();
        
        try {
            //cmd("server", "--help");
            
//            cmd("server", "--zk=local", "--zkData=/home/afs/tmp/ZKD", "--zkPort=2188");
//            cmd("server", "--zk=mem");
//            cmd("server", "--mem");
//            cmd("server", "--base=");
            cmd("server", "--mem", "--port="+PORT);
            
            String URL = "http://localhost:"+PORT+"/";
            
            // Test stuff.
//            cmd("ls", "--server=http://localhost:1066/");
            cmd("mk", "--server="+URL, "ABC");
            cmd("ls", "--server="+URL);
            cmd("rm", "--server="+URL, "ABC");
            cmd("ls", "--server="+URL);
            cmd("mk", "--server="+URL, "ABC");
            
            cmdq("append", "--server="+URL, "--log=ABC", "/home/afs/tmp/data.rdfp");
            
            cmdq("fetch", "--server="+URL, "--log=ABC", "1");
            
            // Now access the server via the API.
            DeltaLink dLink = DeltaLinkHTTP.connect(URL);
            DataSourceDescription dsd = dLink.getDataSourceDescriptionByName("ABC");
            DeltaLog log = new DeltaLog(dLink, dsd.getId());
            PatchLogInfo info = log.info();
            System.out.println();
            System.out.println(info);
            
            
        } catch (Throwable th) {   
            th.printStackTrace();
        }
    }
    
    
    private static void cmd(String...args) {
        dcmd.main(args);
    }

    private static void cmdq(String...args) {
        execNoOutput(()->cmd(args));
    }

}
