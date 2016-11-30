package com.dataloom.authorization;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import com.dataloom.authorization.requests.Principal;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt; 
 *
 */
public class AceKey {
    private final UUID                objectId;
    private final SecurableObjectType objectType;
    private final Principal           principal;

    public AceKey( UUID objectId, SecurableObjectType objectType, Principal principal ) {
        this.objectId = checkNotNull( objectId );
        this.objectType = checkNotNull( objectType );
        this.principal = checkNotNull( principal );
    }

    public UUID getObjectId() {
        return objectId;
    }

    public SecurableObjectType getObjectType() {
        return objectType;
    }

    public Principal getPrincipal() {
        return principal;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( objectId == null ) ? 0 : objectId.hashCode() );
        result = prime * result + ( ( objectType == null ) ? 0 : objectType.hashCode() );
        result = prime * result + ( ( principal == null ) ? 0 : principal.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof AceKey ) ) {
            return false;
        }
        AceKey other = (AceKey) obj;
        if ( objectId == null ) {
            if ( other.objectId != null ) {
                return false;
            }
        } else if ( !objectId.equals( other.objectId ) ) {
            return false;
        }
        if ( objectType != other.objectType ) {
            return false;
        }
        if ( principal == null ) {
            if ( other.principal != null ) {
                return false;
            }
        } else if ( !principal.equals( other.principal ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "AccessControlEntryKey [objectId=" + objectId + ", objectType=" + objectType + ", userId=" + principal
                + "]";
    }
}
