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

package org.seaborne.delta.lib;

import java.io.FilterInputStream ;
import java.io.IOException ;
import java.io.InputStream ;
import java.security.MessageDigest ;

public class DigestInputStream extends FilterInputStream {

    private final MessageDigest digest ;
    
    protected DigestInputStream(InputStream in, MessageDigest digest) {
        super(in) ;
        //this.other = in ;
        this.digest = digest ; 
    }

    // Intercept to pass to the digest engine.
    
    @Override
    public int read() throws IOException {
        // digest
        return in.read() ; 
    }
    
    @Override
    public int read(byte b[]) throws IOException {
        // digest
        return in.read(b);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        // digest
        return in.read(buf, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        throw new UnsupportedOperationException() ;
    }

    @Override
    public void mark(int limit) {
        throw new UnsupportedOperationException() ;
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException() ;
    }

    public byte[] getDigest() {
        return digest.digest() ;
    }

    public void resetDigest() {
        digest.reset() ;
    }
}
