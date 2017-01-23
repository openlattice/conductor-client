package com.kryptnostic.datastore.exceptions;

public class ResourceNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 9036215896394849079L;

    public ResourceNotFoundException( String msg ) {
        super( msg );
    }
}