package com.dataloom.graph.core.objects;

import java.util.UUID;

public class LoomEdge {
    private UUID graphId;
    private EdgeKey key;
    
    private EdgeLabel label;

    public LoomEdge( UUID graphId, EdgeKey key, EdgeLabel label ) {
        this.graphId = graphId;
        this.key = key;
        this.label = label;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public EdgeKey getKey() {
        return key;
    }

    public EdgeLabel getLabel() {
        return label;
    }

    /*
     * Helper methods
     */
    public UUID getSrcId(){
        return key.getSrcId();
    }

    public UUID getDstId(){
        return key.getDstId();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( graphId == null ) ? 0 : graphId.hashCode() );
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LoomEdge other = (LoomEdge) obj;
        if ( graphId == null ) {
            if ( other.graphId != null ) return false;
        } else if ( !graphId.equals( other.graphId ) ) return false;
        if ( key == null ) {
            if ( other.key != null ) return false;
        } else if ( !key.equals( other.key ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "LoomEdge [graphId=" + graphId + ", key=" + key + ", label=" + label + "]";
    }

}