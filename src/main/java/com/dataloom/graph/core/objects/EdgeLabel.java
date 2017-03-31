package com.dataloom.graph.core.objects;

import java.util.UUID;

import com.dataloom.data.EntityKey;

public class EdgeLabel {
    private EntityKey reference;

    private UUID      srcType;
    private UUID      dstType;

    public EdgeLabel( EntityKey reference, UUID srcType, UUID dstType ) {
        this.reference = reference;
        this.srcType = srcType;
        this.dstType = dstType;
    }

    public EntityKey getReference() {
        return reference;
    }

    public UUID getSrcType() {
        return srcType;
    }

    public UUID getDstType() {
        return dstType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( dstType == null ) ? 0 : dstType.hashCode() );
        result = prime * result + ( ( reference == null ) ? 0 : reference.hashCode() );
        result = prime * result + ( ( srcType == null ) ? 0 : srcType.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        EdgeLabel other = (EdgeLabel) obj;
        if ( dstType == null ) {
            if ( other.dstType != null ) return false;
        } else if ( !dstType.equals( other.dstType ) ) return false;
        if ( reference == null ) {
            if ( other.reference != null ) return false;
        } else if ( !reference.equals( other.reference ) ) return false;
        if ( srcType == null ) {
            if ( other.srcType != null ) return false;
        } else if ( !srcType.equals( other.srcType ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "EdgeLabel [reference=" + reference + ", srcType=" + srcType + ", dstType=" + dstType + "]";
    }

}
