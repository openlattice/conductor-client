package com.dataloom.edm.exceptions;

public class TypeExistsException extends RuntimeException {
    private static final long serialVersionUID = -3630252810504431087L;
    public TypeExistsException(String message) {
        super(message);
    }
}
