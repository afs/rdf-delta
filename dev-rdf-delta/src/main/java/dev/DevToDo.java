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
    // DeltaClient -> DatasetGraphChanges

    // Tokens : test with javacc parser
    // javacc tokenizer:
    //   prefix name in ^^xsd:foo. (what does TokenizerText do? SubToken)
    //   _: <WS> to be parse error.
    //   escapes in pnames: "a:b\\#c"
    //  "0xXYZ"
    //  ? abc -> varname ""
    //  "??x"
    //  "''@lang- "
    //  String types: String()
    // 
    //  Differences: token backtracking.
    //     0xXYZ is integer 0 (!)
    //  javacctokeizer has "push token" not "emit token".
    //    Compounds like RDFLiterals.
    
    // Documentation
    
    // Extract polling support to DeltaClient.
    // Assembler to have a DeltaClient.

    // RDF Git.
    // Checksums
    // Binary format
    
    // ---- dev tasks
    // Patch
    // "_" for default graph
    
    // 
    // DatasetGraphBuffering
    // 
    // -- TIO
    //   Stream<Tuple<Token>>
    //   TokenizerJavacc - less of a hack. More tokens like "_"
    //   Clarify rule.  
    //     Tokens or Nodes (= Tokens).
    //     Tuples() as small special part.
    //     Tokens to carry Nodes?
    //     STRING1, STRING2 vs STRING
    //     Complete and check tokenizer.
    //     Node vs Token e.g. for VAR
    
    // rdf patch:
    // Headers.
    // Name for a patch. RDFPatch (free from library),
    // "_" for default graph
    
}
