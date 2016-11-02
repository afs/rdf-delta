package org.seaborne.delta;

public class DeltaException extends RuntimeException
{
    public DeltaException()                          { super() ; }
    public DeltaException(String msg)                { super(msg) ; }
    public DeltaException(Throwable th)              { super(th) ; }
    public DeltaException(String msg, Throwable th)  { super(msg, th) ; }
}
