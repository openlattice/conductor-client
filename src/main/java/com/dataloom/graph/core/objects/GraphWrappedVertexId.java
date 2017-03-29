package com.dataloom.graph.core.objects;

import java.util.UUID;

public class GraphWrappedVertexId {
    private UUID graphId;
    private UUID vertexId;

    public GraphWrappedVertexId( UUID graphId, UUID vertexId ) {
        this.graphId = graphId;
        this.vertexId = vertexId;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public UUID getVertexId() {
        return vertexId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( graphId == null ) ? 0 : graphId.hashCode() );
        result = prime * result + ( ( vertexId == null ) ? 0 : vertexId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        GraphWrappedVertexId other = (GraphWrappedVertexId) obj;
        if ( graphId == null ) {
            if ( other.graphId != null ) return false;
        } else if ( !graphId.equals( other.graphId ) ) return false;
        if ( vertexId == null ) {
            if ( other.vertexId != null ) return false;
        } else if ( !vertexId.equals( other.vertexId ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "GraphWrappedVertexId [graphId=" + graphId + ", vertexId=" + vertexId + "]";
    }

}
