package com.dataloom.graph.core.objects;

import java.util.UUID;

import com.dataloom.data.EntityKey;

public class GraphWrappedEntityKey {
    private UUID      graphId;
    private EntityKey entityKey;

    public GraphWrappedEntityKey( UUID graphId, EntityKey entityKey ) {
        this.graphId = graphId;
        this.entityKey = entityKey;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public EntityKey getEntityKey() {
        return entityKey;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( entityKey == null ) ? 0 : entityKey.hashCode() );
        result = prime * result + ( ( graphId == null ) ? 0 : graphId.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        GraphWrappedEntityKey other = (GraphWrappedEntityKey) obj;
        if ( entityKey == null ) {
            if ( other.entityKey != null ) return false;
        } else if ( !entityKey.equals( other.entityKey ) ) return false;
        if ( graphId == null ) {
            if ( other.graphId != null ) return false;
        } else if ( !graphId.equals( other.graphId ) ) return false;
        return true;
    }

    @Override
    public String toString() {
        return "GraphWrappedEntityKey [graphId=" + graphId + ", entityKey=" + entityKey + "]";
    }

}
