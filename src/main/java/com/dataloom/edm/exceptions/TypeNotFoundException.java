package com.dataloom.edm.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus( HttpStatus.NOT_FOUND )
public class TypeNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 6520676310933990670L;

    public TypeNotFoundException( String message ) {
        super( message );
    }
}
