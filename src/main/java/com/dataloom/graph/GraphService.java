package com.dataloom.graph;

import com.dataloom.data.EntityKey;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public interface GraphService {
    EntityKey getEdge( Edge edge );

    void addEdge( Edge edge, EntityKey vertex );

    void removeEdge( Edge edge );
}
