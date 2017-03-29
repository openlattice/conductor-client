package com.dataloom.graph.core.objects;

import java.util.UUID;

public class GraphWrappedEdgeKey {
    private UUID    graphId;
    private EdgeKey edgeKey;

    public GraphWrappedEdgeKey( UUID graphId, EdgeKey edgeKey ) {
        this.graphId = graphId;
        this.edgeKey = edgeKey;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public EdgeKey getEdgeKey() {
        return edgeKey;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( edgeKey == null ) ? 0 : edgeKey.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        GraphWrappedEdgeKey other = (GraphWrappedEdgeKey) obj;
        if ( edgeKey == null ) {
            if ( other.edgeKey != null ) return false;
        } else if ( !edgeKey.equals( other.edgeKey ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "GraphWrappedEdgeKey [edgeKey=" + edgeKey + "]";
    }

}
