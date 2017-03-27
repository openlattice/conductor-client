package com.dataloom.graph.core.impl;

import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

public class LoomEdge extends LabeledElement {
    private LoomVertex src;
    private LoomVertex dst;
    private UUID       timeId;

    private EdgeKey    key;

    public LoomEdge( UUID graphId, LoomVertex src, LoomVertex dst, Label label ) {
        super( graphId, label );
        this.src = src;
        this.dst = dst;
        this.timeId = UUIDs.timeBased();
        this.key = new EdgeKey( src.getKey(), dst.getKey(), timeId );
    }

    public LoomVertex getSrc() {
        return src;
    }

    public LoomVertex getDst() {
        return dst;
    }

    public UUID getTimeId() {
        return timeId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( dst == null ) ? 0 : dst.hashCode() );
        result = prime * result + ( ( src == null ) ? 0 : src.hashCode() );
        result = prime * result + ( ( timeId == null ) ? 0 : timeId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LoomEdge other = (LoomEdge) obj;
        if ( dst == null ) {
            if ( other.dst != null ) return false;
        } else if ( !dst.equals( other.dst ) ) return false;
        if ( src == null ) {
            if ( other.src != null ) return false;
        } else if ( !src.equals( other.src ) ) return false;
        if ( timeId == null ) {
            if ( other.timeId != null ) return false;
        } else if ( !timeId.equals( other.timeId ) ) return false;
        return true;
    }

    @Override
    public EdgeKey getKey() {
        // TODO Auto-generated method stub
        return key;
    }

}