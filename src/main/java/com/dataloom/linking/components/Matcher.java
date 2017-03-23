package com.dataloom.linking.components;

import java.util.Set;
import java.util.UUID;

import com.dataloom.linking.Entity;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.SetMultimap;

/**
 * Basic Matcher interface.
 * 
 * An initialized Matcher instance should have information about the linking sets and the linking properties.
 * 
 * @author Ho Chung Siu
 *
 */
public interface Matcher {

    public static final double threshold = 0.1;

    /**
     * Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In
     * particular, only identical property types are linked on.
     * 
     * @param entitySetsWithSyncIds
     * @param linkIndexedByPropertyTypes
     * @param linkIndexedByEntitySets
     */
    default void setLinking(
            Set<UUID> linkingEntitySets,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {}

    /**
     * Given an unordered pair of entities, should return a distance of the two entities (between 0 to 1). The lower the
     * distance, the more similar they are.
     * 
     * @param entityPair
     * @return
     */
    public double dist( UnorderedPair<Entity> entityPair );

    /**
     * Given an unordered pair of entities, should return whether the two entities match. The default implementation is
     * when the distance goes below threshold, then the two entity are considered a match.
     * 
     * @param entityPair
     * @return
     */
    default boolean match( UnorderedPair<Entity> entityPair ) {
        return dist( entityPair ) < threshold;
    }

}
