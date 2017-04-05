package com.dataloom.graph.core.objects;

import java.util.UUID;

import com.dataloom.data.EntityKey;

/**
 * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
 * destination vertexId, and the entity key referencing the edge in the edge entity set.
 * 
 * @author Ho Chung Siu
 *
 */
public class EdgeKey {
    private UUID srcId;
    private UUID dstId;
    private EntityKey reference;

    public EdgeKey( UUID srcId, UUID dstId, EntityKey reference ) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.reference = reference;
    }

    public UUID getSrcId() {
        return srcId;
    }

    public UUID getDstId() {
        return dstId;
    }

    public EntityKey getReference() {
        return reference;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( dstId == null ) ? 0 : dstId.hashCode() );
        result = prime * result + ( ( reference == null ) ? 0 : reference.hashCode() );
        result = prime * result + ( ( srcId == null ) ? 0 : srcId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EdgeKey other = (EdgeKey) obj;
        if ( dstId == null ) {
            if ( other.dstId != null ) return false;
        } else if ( !dstId.equals( other.dstId ) ) return false;
        if ( reference == null ) {
            if ( other.reference != null ) return false;
        } else if ( !reference.equals( other.reference ) ) return false;
        if ( srcId == null ) {
            if ( other.srcId != null ) return false;
        } else if ( !srcId.equals( other.srcId ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "EdgeKey [srcId=" + srcId + ", dstId=" + dstId + ", reference=" + reference + "]";
    }

}
