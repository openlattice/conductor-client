package com.dataloom.graph.core.objects;

import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.dataloom.graph.edge.EdgeKey;

public class LoomEdgeKey {
    private EdgeKey key;

    private UUID      srcType;
    private UUID      dstType;

    public LoomEdgeKey( EdgeKey key, UUID srcType, UUID dstType ) {
        this.key = key;
        this.srcType = srcType;
        this.dstType = dstType;
    }

    public EdgeKey getKey() {
        return key;
    }

    public UUID getSrcType() {
        return srcType;
    }

    public UUID getDstType() {
        return dstType;
    }

    /*
     * Helper method
     */
    public EntityKey getReference(){
        return key.getReference();
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( dstType == null ) ? 0 : dstType.hashCode() );
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        result = prime * result + ( ( srcType == null ) ? 0 : srcType.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LoomEdgeKey other = (LoomEdgeKey) obj;
        if ( dstType == null ) {
            if ( other.dstType != null ) return false;
        } else if ( !dstType.equals( other.dstType ) ) return false;
        if ( key == null ) {
            if ( other.key != null ) return false;
        } else if ( !key.equals( other.key ) ) return false;
        if ( srcType == null ) {
            if ( other.srcType != null ) return false;
        } else if ( !srcType.equals( other.srcType ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "LoomEdgeKey [key=" + key + ", srcType=" + srcType + ", dstType=" + dstType + "]";
    }

}