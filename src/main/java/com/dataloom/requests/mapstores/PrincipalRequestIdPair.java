package com.dataloom.requests.mapstores;

import java.io.Serializable;
import java.util.UUID;

import com.dataloom.authorization.Principal;

public class PrincipalRequestIdPair implements Serializable {
    private static final long serialVersionUID = -8095855430605074329L;

    private Principal         user;
    private UUID              requestId;

    public PrincipalRequestIdPair( Principal user, UUID requestId ) {
        this.user = user;
        this.requestId = requestId;
    }

    public Principal getUser() {
        return user;
    }

    public UUID getRequestId() {
        return requestId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( requestId == null ) ? 0 : requestId.hashCode() );
        result = prime * result + ( ( user == null ) ? 0 : user.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        PrincipalRequestIdPair other = (PrincipalRequestIdPair) obj;
        if ( requestId == null ) {
            if ( other.requestId != null ) return false;
        } else if ( !requestId.equals( other.requestId ) ) return false;
        if ( user == null ) {
            if ( other.user != null ) return false;
        } else if ( !user.equals( other.user ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "PrincipalRequestIdPair [principal=" + user + ", requestId=" + requestId + "]";
    }

}
