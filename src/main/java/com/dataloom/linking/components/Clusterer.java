package com.dataloom.linking.components;

import java.util.UUID;

/**
 * Basic Clusterer interface.
 * 
 * An initialized Clusterer instance should have the weighted graph for clustering.
 * @author Ho Chung Siu
 *
 */
public interface Clusterer {
    
    /**
     * Set the linkedEntitySetId, equivalently the edge graph Id.
     */
    public void setId( UUID linkedEntitySetId );

    /**
     * 
     * @return
     */
    public void cluster();
}
