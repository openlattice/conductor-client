package com.dataloom.graph;

import com.dataloom.data.EntityKey;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LinkingEdge extends DirectedEdge {
    private final UUID graphId;

    public LinkingEdge( UUID graphId, EntityKey src, EntityKey dst ) {
        super( GraphUtil.min( src, dst ), GraphUtil.max( src, dst ) );
        this.graphId = graphId;
    }

    public UUID getGraphId() {
        return graphId;
    }
}
