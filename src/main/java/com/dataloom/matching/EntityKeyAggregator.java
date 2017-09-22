package com.dataloom.matching;

import com.dataloom.data.EntityKey;
import com.google.common.collect.Sets;
import com.hazelcast.aggregation.Aggregator;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EntityKeyAggregator extends Aggregator<Map.Entry<EntityKey, UUID>, Set<EntityKey>> {
    private Set<EntityKey> entityKeys;

    public EntityKeyAggregator() {
        this.entityKeys = Sets.newHashSet();
    }

    @Override public void accumulate( Map.Entry<EntityKey, UUID> input ) {
        entityKeys.add( input.getKey() );
    }

    @Override public void combine( Aggregator aggregator ) {
        if ( aggregator instanceof EntityKeyAggregator ) {
            entityKeys.addAll( ( (EntityKeyAggregator) aggregator ).entityKeys );
        }
    }

    @Override public Set<EntityKey> aggregate() {
        return entityKeys;
    }

}
