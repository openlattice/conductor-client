package com.dataloom.graph;

import com.dataloom.data.EntityKey;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class DirectedEdge implements Edge {
    private final EntityKey src;
    private final EntityKey dst;

    public DirectedEdge( EntityKey src, EntityKey dst ) {
        this.src = src;
        this.dst = dst;
    }

    @Override
    public EntityKey getSource() {
        return src;
    }

    @Override
    public EntityKey getDestination() {
        return dst;
    }
}
