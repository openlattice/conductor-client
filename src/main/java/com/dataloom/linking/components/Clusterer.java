package com.dataloom.linking.components;

import com.dataloom.linking.WeightedLinkingEdge;
import java.util.UUID;

/**
 * Basic Clusterer interface.
 * 
 * @author Ho Chung Siu
 *
 */
public interface Clusterer {

    public void cluster( UUID graphId, WeightedLinkingEdge bottom, WeightedLinkingEdge top );
}
