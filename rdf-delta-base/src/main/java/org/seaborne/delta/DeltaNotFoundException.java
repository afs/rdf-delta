package org.seaborne.delta;

import org.apache.jena.web.HttpSC;

public class DeltaNotFoundException extends DeltaBadRequestException
{
    public DeltaNotFoundException(String msg)                { super(HttpSC.NOT_FOUND_404, msg) ; }
}
