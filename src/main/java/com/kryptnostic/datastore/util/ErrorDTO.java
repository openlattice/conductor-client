package com.kryptnostic.datastore.util;

public class ErrorDTO {
    private String type;
    private String message;

    public ErrorDTO( String type, String message ) {
        this.type = type;
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "ErrorDTO [type=" + type + ", message=" + message + "]";
    }

}