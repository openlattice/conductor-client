package com.dataloom.graph.core.impl;

import java.util.UUID;

public class LoomVertex extends LabeledElement {

    public LoomVertex( UUID graphId, Label label ) {
        super( graphId, label );
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        return super.equals( obj );
    }

    @Override
    public UUID getKey() {
        return label.getId();
    }

}
