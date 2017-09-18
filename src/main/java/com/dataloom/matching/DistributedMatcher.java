package com.dataloom.matching;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.data.EntityKey;
import com.dataloom.data.hazelcast.EntitySets;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.services.EdmManager;

public class DistributedMatcher {
    private EdmManager                     dms;

    private SetMultimap<UUID, UUID>        linkIndexedByEntitySets;
    private Map<UUID, UUID>                linkingEntitySetsWithSyncId;
    private Set<UUID>                      linkingES;
    private Map<FullQualifiedName, String> propertyTypeIdIndexedByFqn;
    private final IMap<EntityKey, UUID>    ids;

    public DistributedMatcher(
            HazelcastInstance hazelcast,
            EdmManager dms ) {
        this.ids = hazelcast.getMap( HazelcastMap.IDS.name() );
        this.dms = dms;
    }

    public double match( UUID graphId ) {
        double[] lightest = { Double.MAX_VALUE };
        linkingES.parallelStream().forEach( entitySetId -> {
            double lightestForES = matchEntitySet( graphId, entitySetId );
            if ( lightestForES < lightest[ 0 ] ) {
                lightest[ 0 ] = lightestForES;
            }
        } );
        return lightest[ 0 ];
    }

    private double matchEntitySet( UUID graphId, UUID entitySetId ) {
        UUID syncId = linkingEntitySetsWithSyncId.get( entitySetId );
        Set<UUID> propertiesSet = ImmutableSet.copyOf( linkIndexedByEntitySets.get( entitySetId ) );
        Map<UUID, PropertyType> authorizedPropertyTypes = propertiesSet.stream()
                .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );

        return ids.aggregate( new MatchingAggregator(
                graphId,
                linkingEntitySetsWithSyncId,
                authorizedPropertyTypes,
                propertyTypeIdIndexedByFqn ),
                EntitySets.filterByEntitySetIdAndSyncId( entitySetId, syncId ) );
    }

    public void setLinking(
            Map<UUID, UUID> linkingEntitySetsWithSyncId,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {
        this.linkIndexedByEntitySets = linkIndexedByEntitySets;
        this.linkingEntitySetsWithSyncId = linkingEntitySetsWithSyncId;
        this.linkingES = new HashSet<>( linkingEntitySetsWithSyncId.keySet() );
        this.propertyTypeIdIndexedByFqn = getPropertyTypeIdIndexedByFqn( linkIndexedByPropertyTypes.keySet() );
    }

    private Map<FullQualifiedName, String> getPropertyTypeIdIndexedByFqn( Set<UUID> propertyTypeIds ) {
        return propertyTypeIds.stream()
                .collect( Collectors.toMap( id -> dms.getPropertyType( id ).getType(), id -> id.toString() ) );
    }

}
