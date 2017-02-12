package com.dataloom.graph;

import com.dataloom.data.EntityKey;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public interface Edge {
    EntityKey getSource();

    EntityKey getDestination();
}
