package com.dataloom.linking.components;

import java.util.Map;
import java.util.UUID;

import com.dataloom.linking.Entity;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.Multimap;
/**
 * Basic Blocker interface. 
 * 
 * An initialized Blocker instance should have information about the linking sets and the linking properties.
 * @author Ho Chung Siu
 *
 */
public interface Blocker {

    public void setLinking( Map<UUID, UUID> entitySetsWithSyncIds, Multimap<UUID, UUID> linkingMap );
    
    public Iterable<UnorderedPair<Entity>> block();
}
