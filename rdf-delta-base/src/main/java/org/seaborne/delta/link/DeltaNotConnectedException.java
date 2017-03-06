package org.seaborne.delta.link;

import org.seaborne.delta.DeltaException;

public class DeltaNotConnectedException extends DeltaException
{
    public DeltaNotConnectedException()                          { super() ; }
    public DeltaNotConnectedException(String msg)                { super(msg) ; }
    public DeltaNotConnectedException(Throwable th)              { super(th) ; }
    public DeltaNotConnectedException(String msg, Throwable th)  { super(msg, th) ; }
}
