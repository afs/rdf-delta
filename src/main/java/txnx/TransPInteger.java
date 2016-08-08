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

package txnx;

import java.math.BigInteger ;

import org.apache.jena.system.Txn ;

/** */
public class TransPInteger extends TransPBlob<BigInteger>{

    public TransPInteger(String fn) {
        super(fn, fn+".jrnl") ;
    }

    @Override
    protected BigInteger getUninitalized() {
        return BigInteger.ZERO ;
    }

    @Override
    protected byte[] toBytes(BigInteger number) {
        return number.toByteArray() ; 
    }

    @Override
    protected BigInteger fromBytes(byte[] bytes) {
        if ( bytes == null || bytes.length == 0 )
            return BigInteger.ZERO ;
        return new BigInteger(bytes) ;
    }

    @Override
    protected BigInteger snapshot(BigInteger object) {
        return object ;
    }
    
    public void inc() {
        Txn.execWrite(this, ()->getDataState().add(BigInteger.ONE)) ;
    }
}
