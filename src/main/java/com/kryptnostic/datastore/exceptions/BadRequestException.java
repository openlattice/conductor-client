package com.kryptnostic.datastore.exceptions;

public class BadRequestException extends RuntimeException {
    private static final long serialVersionUID = 9049360916124505696L;

    public BadRequestException( String msg ) {
        super( msg );
    }
}
