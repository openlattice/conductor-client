package com.dataloom.graph.core.objects;

import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

/**
 * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
 * destination vertexId, and syncid for the edge.
 * 
 * @author Ho Chung Siu
 *
 */
public class EdgeKey {
    private UUID srcId;
    private UUID dstId;
    private UUID syncId;

    public EdgeKey( UUID srcId, UUID dstId ) {
        this( srcId, dstId, UUIDs.timeBased() );
    }

    /*
     * Only for deserialization; the time uuid should not be provided in any other case.
     */
    public EdgeKey( UUID srcId, UUID dstId, UUID syncId ) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.syncId = syncId;
    }

    public UUID getSrcId() {
        return srcId;
    }

    public UUID getDstId() {
        return dstId;
    }

    public UUID getSyncId() {
        return syncId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( dstId == null ) ? 0 : dstId.hashCode() );
        result = prime * result + ( ( srcId == null ) ? 0 : srcId.hashCode() );
        result = prime * result + ( ( syncId == null ) ? 0 : syncId.hashCode() );
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
        if ( srcId == null ) {
            if ( other.srcId != null ) return false;
        } else if ( !srcId.equals( other.srcId ) ) return false;
        if ( syncId == null ) {
            if ( other.syncId != null ) return false;
        } else if ( !syncId.equals( other.syncId ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "EdgeKey [srcId=" + srcId + ", dstId=" + dstId + ", syncId=" + syncId + "]";
    }

}
