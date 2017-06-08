package org.seaborne.delta.link;

import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.DeltaHttpException ;

public class DeltaNotRegisteredException extends DeltaHttpException
{
    public DeltaNotRegisteredException()                          { this("Not registered") ; }
    public DeltaNotRegisteredException(String msg)                { super(HttpSC.UNAUTHORIZED_401, msg) ; }
}
