package com.dataloom.blocking;

import com.dataloom.data.EntityKey;

import java.util.UUID;

public class GraphEntityPair {

    private final UUID graphId;
    private final EntityKey entityKey;

    public GraphEntityPair( UUID graphId, EntityKey entityKey ) {
        this.graphId = graphId;
        this.entityKey = entityKey;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public EntityKey getEntityKey() {
        return entityKey;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o )
            return true;
        if ( o == null || getClass() != o.getClass() )
            return false;

        GraphEntityPair that = (GraphEntityPair) o;

        if ( graphId != null ? !graphId.equals( that.graphId ) : that.graphId != null )
            return false;
        return entityKey != null ? entityKey.equals( that.entityKey ) : that.entityKey == null;
    }

    @Override public int hashCode() {
        int result = graphId != null ? graphId.hashCode() : 0;
        result = 31 * result + ( entityKey != null ? entityKey.hashCode() : 0 );
        return result;
    }
}
