package com.dataloom.graph.core.objects;

import com.dataloom.data.EntityKey;

public class VertexLabel {
    private EntityKey reference;

    public VertexLabel( EntityKey reference ) {
        this.reference = reference;
    }

    public EntityKey getReference() {
        return reference;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( reference == null ) ? 0 : reference.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        VertexLabel other = (VertexLabel) obj;
        if ( reference == null ) {
            if ( other.reference != null ) return false;
        } else if ( !reference.equals( other.reference ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "VertexLabel [reference=" + reference + "]";
    }

}
