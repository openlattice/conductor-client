package com.dataloom.edm.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus( HttpStatus.CONFLICT )
public class TypeExistsException extends RuntimeException {
    private static final long serialVersionUID = -3630252810504431087L;

    public TypeExistsException( String message ) {
        super( message );
    }
}
