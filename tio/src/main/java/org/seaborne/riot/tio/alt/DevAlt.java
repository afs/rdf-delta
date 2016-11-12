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

package org.seaborne.riot.tio.alt;

import java.io.InputStream ;
import java.io.StringReader ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.riot.tokens.Token ;
import org.apache.jena.riot.tokens.Tokenizer ;
import org.apache.jena.riot.tokens.TokenizerFactory ;
import org.seaborne.riot.tio.TupleReader ;
import org.seaborne.riot.tio.impl.TupleReaderTokenizer ;

public class DevAlt {
    
    public static void main(String ...a) {
        // Necessary but why?
        // Otherwise TokenizerJavacc causes "broken term"
        
        String s = "<xyz\tabc>" ;
        System.out.println("Input:|"+s+"|"); 

        try {
            Tokenizer tok1 = new TokenizerJavacc(new StringReader(s)) ;
            print("javacc", tok1);
        } catch (RiotParseException ex) {
            ex.printStackTrace(System.out);
        }
        
        try {
            Tokenizer tok2 = TokenizerFactory.makeTokenizer(new StringReader(s));
            print("TokenizerText", tok2);
        } catch (RiotParseException ex) {
            ex.printStackTrace(System.out);
        }

    }
    
    private static void print(String label, Tokenizer tok) {
        TupleReader tr = new TupleReaderTokenizer(tok) ;
        System.out.println(label);
        System.out.flush();
        tr.forEach(t->System.out.printf("  >> %s\n", t));
    }   

    public static void main1(String ...a) {
        InputStream in = IO.openFile("data") ;
        TupleReaderJavacc trj = new TupleReaderJavacc(in) ;
        
        for ( Tuple<Token> t : trj ) {
            System.out.println(">>"+t);
        }
        
        System.out.println("DONE");
    }
    

}
