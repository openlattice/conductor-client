package com.dataloom.linking.components;

import java.util.Set;

import com.dataloom.data.EntityKey;

/**
 * Basic Clusterer interface.
 * 
 * An initialized Clusterer instance should have the weighted graph for clustering. More precisely, it should have the set of entity keys (the vertex set), and a distance function between the vertices.
 * @author Ho Chung Siu
 *
 */
public interface Clusterer {

    /**
     * 
     * @return
     */
    public Iterable<Set<EntityKey>> cluster();
}
