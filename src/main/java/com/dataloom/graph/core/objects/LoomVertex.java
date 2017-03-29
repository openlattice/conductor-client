package com.dataloom.graph.core.objects;

import java.util.UUID;

public class LoomVertex {

    private UUID graphId;
    private UUID key;
    ;
    private VertexLabel label;
    
    public LoomVertex( UUID graphId, UUID key, VertexLabel label ) {
        this.graphId = graphId;
        this.key = key;
        this.label = label;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public UUID getVertexId() {
        return key;
    }

    public VertexLabel getLabel() {
        return label;
    }

}
