package com.dataloom.authorization;

import java.util.UUID;

import com.google.common.base.Preconditions;

/**
 * This class is intended to be used as the key for the hazelcast map storing permission information.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public class AclKey {
    private final SecurableObjectType objectType;
    private final UUID                objectId;

    public AclKey( SecurableObjectType objectType, UUID objectId ) {
        this.objectType = Preconditions.checkNotNull( objectType );
        this.objectId = Preconditions.checkNotNull( objectId );
    }

    public UUID getObjectId() {
        return objectId;
    }

    public SecurableObjectType getObjectType() {
        return objectType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( objectId == null ) ? 0 : objectId.hashCode() );
        result = prime * result + ( ( objectType == null ) ? 0 : objectType.hashCode() );
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
        if ( !( obj instanceof AclKey ) ) {
            return false;
        }
        AclKey other = (AclKey) obj;
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
        return true;
    }

    @Override
    public String toString() {
        return "PermissionKey [objectId=" + objectId + ", objectType=" + objectType + "]";
    }

}
