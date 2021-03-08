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

package org.seaborne.delta.cmds;

import java.io.PrintStream;
import java.net.BindException;

import delta.server.DeltaServerCmd;
import org.apache.jena.atlas.io.NullOutputStream;
import org.apache.jena.atlas.web.WebLib;

public class CmdTestLib {

    static PrintStream devnull= new PrintStream(new NullOutputStream());

    static void cmd(String...args) {
        dcmd.main(args);
    }

    static void cmdq(String...args) {
        execNoOutput(()->cmd(args));
    }

    static void execNoOutput(Runnable action) {
        PrintStream x = System.out;
        try {
            System.setOut(devnull);
            action.run();
        } finally { System.setOut(x); }
    }

    public static String server(String... args) {
        return server(args, WebLib.choosePort());
    }

    public static String serverJettyConfig(String... args) {
        return server(args, null);
    }

    private static String server(String[] args, Integer port) {
        int finalPort = (port == null ? 1068 : port);
        String[] serverArgs = (port == null ? new String[0] : new String[] {"--port="+finalPort});

        String[] cmdLine = new String[args.length+serverArgs.length];
        System.arraycopy(args, 0, cmdLine, serverArgs.length, args.length);
        System.arraycopy(serverArgs, 0, cmdLine, 0, serverArgs.length);
        try {
            DeltaServerCmd.server(cmdLine).start();
        } catch (BindException e) {
            throw new RuntimeException(e);
        }
        return "http://localhost:"+finalPort+"/";
    }

}
