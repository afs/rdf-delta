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
    // Documentation
    //   Patch
    //   Protocol/delta
    // Version -> long?
    // Eliminate use of Location and just use Path.

    // Server local "Patch" - relevant?
    //   PatchLog.HistoryEntry - keep jyst meta data and have a "RDFPatch cache".
    
    //PatchLog - conflates "index" and "version" - acceptable?
    //          - revisit HistoryEntry - it keeps patches? LRU cache of patches.  
    // 
    
    // PatchLog.VALIDATE_PATCH_LOG
    // Cache patches, not hold in memory.
    
    // rdf-delta -> Delta
    
    // ** Examples - can be used for testing.
    
    // * Missing is communicating about graphs that have been created and have been deleted and need to appear/disappear in every app server. There are probably other operations that I will find are needed.
    // * Clear-up items: tests for some error cases like out of sequence patches.
    // * Jena assembler integration
    // * Documentation
    // * An unknown is keeping a TBC/DP graph up to date as there is no natural point to check. Probably only on open in TBS, rather than it changing as the user edits it.
    // * EVN/EDG - creating new graphs, removing graphs.
    // * EVN/EDG - How new graph "appear" magically when created via one server and have to appear on all other app servers.

    // StandardOpenOption.CREATE_NEW -- atomic
    
    // ToDo
    // * License header
    // * Examples.
    // * Persistent state : counter+UUID
    // * Test Restart
    // * Disabled filter to requests.
    // * Test apply patch, not in sequence
    
    // * Tests assembler
    // Assembler to have a DeltaClient.
    
    // ------------------------------------------------------------------------
    // Document, "Design" protocol
    // -- History and parenthood.

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

    // -- Others
    // Extract polling support to DeltaClient.

    // DatasetGraphBuffering
    // 
    // -- TIO
    //   Stream<Tuple<Token>>
    //     STRING1, STRING2 vs STRING
}
