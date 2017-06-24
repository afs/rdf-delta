package org.seaborne.delta;

public class DeltaFileException extends DeltaException
{
    public DeltaFileException()                          { super() ; }
    public DeltaFileException(String msg)                { super(msg) ; }
    public DeltaFileException(Throwable th)              { super(th) ; }
    public DeltaFileException(String msg, Throwable th)  { super(msg, th) ; }
}
