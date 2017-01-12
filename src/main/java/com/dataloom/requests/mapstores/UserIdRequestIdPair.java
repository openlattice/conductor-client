package com.dataloom.requests.mapstores;

import java.io.Serializable;
import java.util.UUID;

public class UserIdRequestIdPair implements Serializable {
    private static final long serialVersionUID = -8095855430605074329L;

    private String              userId;
    private UUID              requestId;

    public UserIdRequestIdPair( String userId, UUID requestId ) {
        this.userId = userId;
        this.requestId = requestId;
    }

    public String getUserId() {
        return userId;
    }

    public UUID getRequestId() {
        return requestId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( requestId == null ) ? 0 : requestId.hashCode() );
        result = prime * result + ( ( userId == null ) ? 0 : userId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        UserIdRequestIdPair other = (UserIdRequestIdPair) obj;
        if ( requestId == null ) {
            if ( other.requestId != null ) return false;
        } else if ( !requestId.equals( other.requestId ) ) return false;
        if ( userId == null ) {
            if ( other.userId != null ) return false;
        } else if ( !userId.equals( other.userId ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "UserIdRequestIdPair [userId=" + userId + ", requestId=" + requestId + "]";
    }

}
