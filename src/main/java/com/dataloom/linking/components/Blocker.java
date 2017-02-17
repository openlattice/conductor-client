package com.dataloom.linking.components;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import com.dataloom.linking.Entity;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

/**
 * Basic Blocker interface.
 * 
 * An initialized Blocker instance should have information about the linking sets and the linking properties.
 * 
 * @author Ho Chung Siu
 *
 */
public interface Blocker {

    /**
     * Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In particular, only identical property types are linked on.
     * @param entitySetsWithSyncIds
     * @param linkIndexedByPropertyTypes
     * @param linkIndexedByEntitySets
     */
    public void setLinking( Map<UUID, UUID> entitySetsWithSyncIds,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets );

    public Stream<UnorderedPair<Entity>> block();
}
