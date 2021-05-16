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

import java.util.Objects;

/** Patch log state */
public final /*record*/ class LockState {
    public final Id session;
    public final long ticks;

    private LockState(Id session, long ticks) {
        super();
        this.session = session;
        this.ticks = ticks;
    }

    public final static LockState UNLOCKED = new LockState(null, -1);

    public static LockState create(Id session, long ticks) {
        Objects.requireNonNull(session);
        if ( ticks <= 0 )
            throw new IllegalArgumentException("ticks");
        return new LockState(session, ticks);
    }

    public static boolean isFree(LockState lockState) {
        return lockState == null || lockState == LockState.UNLOCKED ;
    }

    @Override
    public String toString() {
        if ( isFree(this) )
            return "[unlocked]";
        return String.format("[%s, %s]", session, ticks);
    }

//    @Override
//    public int hashCode() {
//        final int prime = 31;
//        int result = 1;
//        result = prime * result + ((session == null) ? 0 : session.hashCode());
//        result = prime * result + (int)(ticks ^ (ticks >>> 32));
//        return result;
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        if ( this == obj )
//            return true;
//        if ( obj == null )
//            return false;
//        if ( getClass() != obj.getClass() )
//            return false;
//        LockState other = (LockState)obj;
//        if ( session == null ) {
//            if ( other.session != null )
//                return false;
//        } else if ( !session.equals(other.session) )
//            return false;
//        if ( ticks != other.ticks )
//            return false;
//        return true;
//    }
}
