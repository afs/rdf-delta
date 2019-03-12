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

package org.seaborne.delta.integration;

import static java.lang.String.format;

import java.io.PrintStream;

public class PrintLib {
    static final PrintStream _out = System.out; 
    static final PrintStream _err = System.err;
    
    public static void println() {
        println(_out);
    }
    
    public static void println(String str) {
        println(_out, str);
    }

    public static void printf(String fmt, Object...args) {
        printf(_out, fmt, args);
    }

    public static void printfln(String fmt, Object...args) {
        printfln(_out, fmt, args);
    }

    public static void err_ln() {
        println(_err);
    }
    
    public static void err_ln(String str) {
        println(_err, str);
    }

    public static void err_f(String fmt, Object...args) {
        printf(_err, fmt, args);
    }

    public static void err_fln(String fmt, Object...args) {
        printfln(_err, fmt, args);
    }

    
    // --------
    
    private static void println(PrintStream out) {
        out.println();
    }
    
    private static void println(PrintStream out, String str) {
        out.println(str);
    }

    private static void printf(PrintStream out, String fmt, Object...args) {
        String text = format(fmt, args);
        out.print(fmt);
    }

    private static void printfln(PrintStream out, String fmt, Object...args) {
        if ( ! fmt.endsWith("\n") )
            fmt = fmt + "\n";
        String text = format(fmt, args);
        out.print(text);
    }

}
