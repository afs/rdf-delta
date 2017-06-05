package org.seaborne.delta.link;

import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.DeltaBadRequestException ;

public class DeltaNotRegisteredException extends DeltaBadRequestException
{
    public DeltaNotRegisteredException()                          { this("Not registered") ; }
    public DeltaNotRegisteredException(String msg)                { super(HttpSC.UNAUTHORIZED_401, msg) ; }
}
