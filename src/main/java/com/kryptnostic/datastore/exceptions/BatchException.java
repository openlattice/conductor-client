package com.kryptnostic.datastore.exceptions;

import java.util.List;

import org.springframework.http.HttpStatus;

import com.kryptnostic.datastore.util.ErrorDTO;
import com.kryptnostic.datastore.util.ErrorsDTO;

public class BatchException extends RuntimeException {
    private static final long serialVersionUID = 7632884063119454460L;
    private ErrorsDTO         errors           = new ErrorsDTO();
    private HttpStatus        statusCode;

    public BatchException( ErrorsDTO errors ) {
        this( errors, HttpStatus.INTERNAL_SERVER_ERROR );
    }

    public BatchException( List<ErrorDTO> list ) {
        this( list, HttpStatus.INTERNAL_SERVER_ERROR );
    }

    public BatchException( ErrorsDTO errors, HttpStatus statusCode ) {
        this.errors = errors;
        this.statusCode = statusCode;
    }

    public BatchException( List<ErrorDTO> list, HttpStatus statusCode ) {
        this.errors.setErrors( list );
        this.statusCode = statusCode;
    }

    public ErrorsDTO getErrors() {
        return errors;
    }

    public void addError( String type, String message ) {
        errors.addError( type, message );
    }

    public void setErrors( ErrorsDTO errors ) {
        this.errors = errors;
    }

    public void setErrors( List<ErrorDTO> list ) {
        this.errors.setErrors( list );
    }

    public HttpStatus getStatusCode() {
        return statusCode;
    }

}
