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

}