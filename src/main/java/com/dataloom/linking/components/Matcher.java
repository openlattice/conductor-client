package com.dataloom.linking.components;

import com.dataloom.linking.Entity;
import com.dataloom.linking.util.UnorderedPair;

/**
 * Basic Matcher interface. 
 * 
 * An initialized Matcher instance should have information about the linking sets and the linking properties.
 * @author Ho Chung Siu
 *
 */
public interface Matcher {

    /**
     * Given an unordered pair of entities, should return the probability where the two entities match.
     * @param entityPair
     * @return
     */
    public double score( UnorderedPair<Entity> entityPair );

    /**
     * Given an unordered pair of entities, should return whether the two entities match.
     * @param entityPair
     * @return
     */
    public boolean match( UnorderedPair<Entity> entityPair );

}
