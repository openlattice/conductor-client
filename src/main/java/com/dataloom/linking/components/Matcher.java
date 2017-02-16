package com.dataloom.linking.components;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.dataloom.linking.Entity;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.Multimap;
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

    public static final double threshold = 0.8;

    /**
     * Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In particular, only identical property types are linked on.
     * @param entitySetsWithSyncIds
     * @param linkIndexedByPropertyTypes
     * @param linkIndexedByEntitySets
     */
    default void setLinking(
            Map<UUID, UUID> entitySetsWithSyncIds,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {
    }

    /**
     * Given an unordered pair of entities, should return the score between 0 to 1 where the two entities match.
     * 
     * @param entityPair
     * @return
     */
    public double score( UnorderedPair<Entity> entityPair );

    /**
     * Given an unordered pair of entities, should return whether the two entities match. The default implementation is
     * when the score goes above threshold, then the two entity are considered a match.
     * 
     * @param entityPair
     * @return
     */
    default boolean match( UnorderedPair<Entity> entityPair ) {
        return score( entityPair ) > threshold;
    }

}
