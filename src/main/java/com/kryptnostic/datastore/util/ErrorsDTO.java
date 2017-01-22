package com.kryptnostic.datastore.util;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ErrorsDTO {
    private List<ErrorDTO> errors;

    public ErrorsDTO() {
        this.errors = new ArrayList<ErrorDTO>();
    }
    
    public ErrorsDTO( String type, String message ){
        this();
        this.addError( type, message );
    }
    
    public void addError( String type, String message ) {
        errors.add( new ErrorDTO( type, message ) );
    }

    public List<ErrorDTO> getErrors() {
        return errors;
    }

    public void setErrors( List<ErrorDTO> errors ) {
        this.errors = errors;
    }

    @Override
    public String toString() {
        return "ErrorsDTO [errors=" + errors + "]";
    }

    @JsonIgnore
    public boolean isEmpty() {
        return errors.isEmpty();
    }
}
