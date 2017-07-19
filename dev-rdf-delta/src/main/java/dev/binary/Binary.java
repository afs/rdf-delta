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

package dev.binary;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.FileOps;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.binary.RDFChangesWriterBinary;
import org.seaborne.patch.binary.RDFPatchReaderBinary;

public class Binary {

    // In RDFPatchOps operations.
    // Naming PatchReader but RDFChangeWriter
    
    public static void main(String[] args) {
        String F = "Patches/patch.rpb" ;
        FileOps.delete(F);
        RDFPatch patch = RDFPatchOps.fileToPatch("data.rdfp");
        RDFChangesWriterBinary.write(patch, F);
        RDFPatch patch1 = RDFPatchReaderBinary.read(IO.openFile(F));
        RDFPatchOps.write(System.out, patch1);
        
        FileOps.delete(F);
        System.out.println("DONE");
        System.exit(0);
    }
}
