package com.dataloom.edm.exceptions;

public class AclKeyConflictException extends RuntimeException {
    private static final long serialVersionUID = -3630252810504431087L;
    public AclKeyConflictException(String message) {
        super(message);
    }
}
