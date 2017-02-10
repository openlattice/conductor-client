package com.dataloom.linking.components;

import com.dataloom.linking.Entity;
import com.dataloom.linking.util.UnorderedPair;

/**
 * Basic Blocker interface. 
 * 
 * An initialized Blocker instance should have information about the linking sets and the linking properties.
 * @author Ho Chung Siu
 *
 */
public interface Blocker {

    public Iterable<UnorderedPair<Entity>> block();
}
