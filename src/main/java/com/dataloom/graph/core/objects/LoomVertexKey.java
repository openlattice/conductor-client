package com.dataloom.graph.core.objects;

import java.util.UUID;

import com.openlattice.data.EntityKey;

public class LoomVertexKey {

    private UUID      key;
    private EntityKey reference;

    public LoomVertexKey( UUID key, EntityKey reference ) {
        this.key = key;
        this.reference = reference;
    }

    public UUID getKey() {
        return key;
    }

    public EntityKey getReference() {
        return reference;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        result = prime * result + ( ( reference == null ) ? 0 : reference.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LoomVertexKey other = (LoomVertexKey) obj;
        if ( key == null ) {
            if ( other.key != null ) return false;
        } else if ( !key.equals( other.key ) ) return false;
        if ( reference == null ) {
            if ( other.reference != null ) return false;
        } else if ( !reference.equals( other.reference ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "LoomVertexKey [key=" + key + ", reference=" + reference + "]";
    }

}
