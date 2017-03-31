package com.dataloom.graph.core.objects;

import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

/**
 * An EdgeKey is the pojo for the primary key of edges table. In the current setting, this is source vertexId,
 * destination vertexId, and timeuuid for time written.
 * 
 * @author Ho Chung Siu
 *
 */
public class EdgeKey {
    private UUID srcId;
    private UUID dstId;
    private UUID timeId;

    public EdgeKey( UUID srcId, UUID dstId ) {
        this( srcId, dstId, UUIDs.timeBased() );
    }

    /*
     * Only for deserialization; the time uuid should not be provided in any other case.
     */
    public EdgeKey( UUID srcId, UUID dstId, UUID timeId ) {
        this.srcId = srcId;
        this.dstId = dstId;
        this.timeId = timeId;
    }

    public UUID getSrcId() {
        return srcId;
    }

    public UUID getDstId() {
        return dstId;
    }

    public UUID getTimeId() {
        return timeId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( dstId == null ) ? 0 : dstId.hashCode() );
        result = prime * result + ( ( srcId == null ) ? 0 : srcId.hashCode() );
        result = prime * result + ( ( timeId == null ) ? 0 : timeId.hashCode() );
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
        if ( timeId == null ) {
            if ( other.timeId != null ) return false;
        } else if ( !timeId.equals( other.timeId ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "EdgeKey [srcId=" + srcId + ", dstId=" + dstId + ", timeId=" + timeId + "]";
    }

}
