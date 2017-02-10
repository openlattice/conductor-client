package com.dataloom.linking.components;

import java.util.Set;
import java.util.UUID;

import com.dataloom.data.EntityKey;
import com.google.common.collect.SetMultimap;

/**
 * Basic Merger interface. 
 * 
 * An initialized Merger instance should have information about the linking sets and the linking properties. 
 * The Merger instance should also compute the set of authorized properties in each involved entity set after linking. If there is no authorized properties in the overall linked entity set, throw an exception.
 * @author Ho Chung Siu
 *
 */
//TODO Maybe worthwhile to move initialization of Merger to the beginning of linking function. If the set of authorized property types in the overall linked entity set is empty, throw an exception to stop the linking.
public interface Merger {

    public Iterable<SetMultimap<UUID, Object>> merge( Set<EntityKey> linkingEntities );
}
