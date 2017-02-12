package com.dataloom.graph;

import com.dataloom.data.EntityKey;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class SimpleEdge extends DirectedEdge {
    public SimpleEdge( EntityKey src, EntityKey dst ) {
        super( GraphUtil.min( src, dst ), GraphUtil.max( src, dst ) );
    }
}
