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

package org.seaborne.delta;

/** Reference to an integer value (as a long) */ 
public interface RefLong {
    
    /** Get the current value */
    public long getInteger();
    
    /** Set the current value */
    public void setInteger(long value);

    /** Increment the current value and return the new value (link {@code ++x} not like {@code X++}) */ 
    public long inc();
}
