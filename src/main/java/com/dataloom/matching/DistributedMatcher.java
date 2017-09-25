package com.dataloom.matching;

import com.dataloom.blocking.BlockingAggregator;
import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.dataloom.blocking.LoadingAggregator;
import com.dataloom.data.EntityKey;
import com.dataloom.data.hazelcast.DataKey;
import com.dataloom.data.hazelcast.EntitySets;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.predicates.LinkingPredicates;
import com.google.common.base.Stopwatch;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedStringSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DistributedMatcher {
    private EdmManager dms;

    private       SetMultimap<UUID, UUID>              linkIndexedByEntitySets;
    private       Map<UUID, UUID>                      linkingEntitySetsWithSyncId;
    private       Set<UUID>                            linkingES;
    private       Map<FullQualifiedName, UUID>         propertyTypeIdIndexedByFqn;
    private final IMap<EntityKey, UUID>                ids;
    private final IMap<DataKey, ByteBuffer>            data;
    private final IMap<GraphEntityPair, LinkingEntity> linkingEntities;
    private final IMap<Set<EntityKey>, UUID>           linkingEntityKeyIdPairs;

    public DistributedMatcher(
            HazelcastInstance hazelcast,
            EdmManager dms ) {
        this.ids = hazelcast.getMap( HazelcastMap.IDS.name() );
        this.data = hazelcast.getMap( HazelcastMap.DATA.name() );
        this.linkingEntities = hazelcast.getMap( HazelcastMap.LINKING_ENTITIES.name() );
        this.linkingEntityKeyIdPairs = hazelcast.getMap( HazelcastMap.LINKING_ENTITY_KEY_ID_PAIRS.name() );
        this.dms = dms;
    }

    public double match( UUID graphId ) {
        System.out.println("........................");
        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = linkIndexedByEntitySets.asMap().entrySet().stream()
                .collect( Collectors.toMap( entry -> entry.getKey(),
                        entry -> entry.getValue().stream()
                                .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) ) ) );

        Stopwatch s = Stopwatch.createStarted();

        data.aggregate( new LoadingAggregator( graphId, authorizedPropertyTypes ),
                EntitySets.filterByEntitySetIdAndSyncIdPairs( linkingEntitySetsWithSyncId ) );
        System.out.println( "t1: " + String.valueOf( s.elapsed( TimeUnit.MILLISECONDS ) ));
        s.reset();
        s.start();

        linkingEntities.aggregate( new BlockingAggregator( graphId, linkingEntitySetsWithSyncId, propertyTypeIdIndexedByFqn ),
                LinkingPredicates.graphId( graphId ) );
        System.out.println( "t2: " + String.valueOf( s.elapsed( TimeUnit.MILLISECONDS ) ));
        return 0.5;
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

    private Map<FullQualifiedName, UUID> getPropertyTypeIdIndexedByFqn( Set<UUID> propertyTypeIds ) {
        return propertyTypeIds.stream()
                .collect( Collectors.toMap( id -> dms.getPropertyType( id ).getType(), id -> id ) );
    }

}
