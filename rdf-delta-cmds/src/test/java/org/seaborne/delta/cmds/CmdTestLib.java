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

import java.io.PrintStream;
import java.net.BindException;

import delta.server.DeltaServerCmd;
import org.apache.jena.atlas.io.NullOutputStream;
import org.seaborne.delta.lib.LibX;

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
        int port = LibX.choosePort();
        String[] serverArgs = {"--port="+port};

        String[] cmdLine = new String[args.length+serverArgs.length];
        System.arraycopy(args, 0, cmdLine, serverArgs.length, args.length);
        System.arraycopy(serverArgs, 0, cmdLine, 0, serverArgs.length);
        try {
            DeltaServerCmd.server(cmdLine).start();
        } catch (BindException e) {
            throw new RuntimeException(e);
        }
        return "http://localhost:"+port+"/";
    }

}
