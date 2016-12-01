package com.dataloom.edm;

import java.util.UUID;

import com.dataloom.authorization.SecurableObjectType;

public class ObjectKey implements Comparable<ObjectKey> {
    private final SecurableObjectType type;
    private final UUID                id;

    public ObjectKey( SecurableObjectType type, UUID id ) {
        this.type = type;
        this.id = id;
    }

    public SecurableObjectType getType() {
        return type;
    }

    public UUID getId() {
        return id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( id == null ) ? 0 : id.hashCode() );
        result = prime * result + ( ( type == null ) ? 0 : type.hashCode() );
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
        if ( !( obj instanceof ObjectKey ) ) {
            return false;
        }
        ObjectKey other = (ObjectKey) obj;
        if ( id == null ) {
            if ( other.id != null ) {
                return false;
            }
        } else if ( !id.equals( other.id ) ) {
            return false;
        }
        if ( type != other.type ) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "ObjectKey [type=" + type + ", id=" + id + "]";
    }

    @Override
    public int compareTo( ObjectKey o ) {
        int c = type.compareTo( o.getType() );
        return c == 0 ? id.compareTo( o.getId() ) : c;
    }

}
