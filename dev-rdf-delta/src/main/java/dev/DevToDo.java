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

package dev;

public class DevToDo {
    // ClientId to "type" Id. 
    
    // Design protocol.
    // "--mem--"
    // See 
    //    AbstractTestDeltaLink.client_01
    //    AbstractTestDeltaConnection.client_01
    
    // DRPC: always include client id after registration
    // Meta level
    /*
     *      { 
     *         "operation":
     *         "arg":
     *         "token":
     *      }
     */
    
    // -- History and parenthood.
    
    // -- Documentation
    
    // TIO
    //   Token getType -> STRING, + getActualType "seen as".
    
    // -- javacc tokenizer:
    //   prefix name in ^^xsd:foo. (what does TokenizerText do? SubToken)
    //   See TestTokenizerJavaccTerms
    //     "0xXYZ"
    //     "''@lang- "

    //  Differences: token backtracking.
    //     0xXYZ is integer 0 (!)
    //  javacctokeizer has "push token" not "emit token".
    //    Compounds like RDFLiterals.

    // __ Others
    // Extract polling support to DeltaClient.
    // Assembler to have a DeltaClient.

    // DatasetGraphBuffering
    // 
    // -- TIO
    //   Stream<Tuple<Token>>
    //     STRING1, STRING2 vs STRING
}
