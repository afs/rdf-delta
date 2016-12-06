package org.seaborne.delta;

public class DeltaConfigException extends DeltaException
{
    public DeltaConfigException()                          { super() ; }
    public DeltaConfigException(String msg)                { super(msg) ; }
    public DeltaConfigException(Throwable th)              { super(th) ; }
    public DeltaConfigException(String msg, Throwable th)  { super(msg, th) ; }
}
