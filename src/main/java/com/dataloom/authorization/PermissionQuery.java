package com.dataloom.authorization;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.spark_project.guava.base.Preconditions;

import com.google.common.base.Optional;

/**
 * Query object for ACLs matching specified filters.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public class PermissionQuery {
    private final Optional<String>         userId;
    private final Set<SecurableObjectType> objectTypes;
    private final Optional<UUID>           objectId;

    public PermissionQuery(
            Optional<String> userId,
            Set<SecurableObjectType> objectType,
            Optional<UUID> objectId ) {
        if ( userId.isPresent() ) {
            Preconditions.checkArgument( StringUtils.isNotBlank( userId.get() ) );
        }
        this.userId = userId;
        this.objectTypes = objectType;
        this.objectId = objectId;
    }

    public PermissionQuery( String userId, Set<SecurableObjectType> objectType, UUID objectId ) {
        this( Optional.of( userId ), objectType, Optional.of( objectId ) );
    }

    public Optional<String> getUserId() {
        return userId;
    }

    public Set<SecurableObjectType> getObjectTypes() {
        return objectTypes;
    }

    public Optional<UUID> getObjectId() {
        return objectId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( objectId == null ) ? 0 : objectId.hashCode() );
        result = prime * result + ( ( objectTypes == null ) ? 0 : objectTypes.hashCode() );
        result = prime * result + ( ( userId == null ) ? 0 : userId.hashCode() );
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
        if ( !( obj instanceof PermissionQuery ) ) {
            return false;
        }
        PermissionQuery other = (PermissionQuery) obj;
        if ( objectId == null ) {
            if ( other.objectId != null ) {
                return false;
            }
        } else if ( !objectId.equals( other.objectId ) ) {
            return false;
        }
        if ( objectTypes != other.objectTypes ) {
            return false;
        }
        if ( userId == null ) {
            if ( other.userId != null ) {
                return false;
            }
        } else if ( !userId.equals( other.userId ) ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "PermissionQuery [userId=" + userId + ", objectType=" + objectTypes + ", objectId=" + objectId + "]";
    }
}
