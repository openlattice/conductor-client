package com.dataloom.graph;

import com.dataloom.graph.mapstores.LinkingVertexKey;
import com.google.common.base.Preconditions;

import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class LinkingEdge {
    private final LinkingVertexKey src;
    private final LinkingVertexKey dst;

    public LinkingEdge( LinkingVertexKey src, LinkingVertexKey dst ) {
        Preconditions.checkArgument( src.getGraphId().equals( dst.getGraphId() ) );
        if ( src.compareTo( dst ) < 0 ) {
            this.src = src;
            this.dst = dst;
        } else {
            this.src = dst;
            this.dst = src;
        }
    }

    public UUID getGraphId() {
        return src.getGraphId();
    }

    public LinkingVertexKey getSrc() {
        return src;
    }

    public LinkingVertexKey getDst() {
        return dst;
    }

}
