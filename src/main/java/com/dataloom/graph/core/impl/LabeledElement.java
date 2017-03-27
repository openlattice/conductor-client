package com.dataloom.graph.core.impl;

import java.util.UUID;

/**
 * Base class for {@link LoomVertex}, {@link LoomEdge}
 * 
 * @author Ho Chung Siu
 *
 */
public abstract class LabeledElement {
    protected UUID  graphId;
    protected Label label;

    public LabeledElement( UUID graphId, Label label ) {
        this.graphId = graphId;
        this.label = label;
    }

    public UUID getGraphId() {
        return graphId;
    }

    public Label getLabel() {
        return label;
    }

    /**
     * {@link #getKey} should return the primary key in the corresponding table (Vertex or Edges)
     * @return
     */
    public abstract Object getKey();

    public Label.Type getType(){
        return label.getType();
    }
}
