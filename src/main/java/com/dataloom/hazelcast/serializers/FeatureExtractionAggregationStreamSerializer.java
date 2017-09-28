package com.dataloom.hazelcast.serializers;

import com.dataloom.blocking.GraphEntityPair;
import com.dataloom.blocking.LinkingEntity;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.matching.FeatureExtractionAggregator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Component
public class FeatureExtractionAggregationStreamSerializer implements SelfRegisteringStreamSerializer<FeatureExtractionAggregator> {
    private ConductorElasticsearchApi elasticsearchApi;

    @Override public Class<? extends FeatureExtractionAggregator> getClazz() {
        return FeatureExtractionAggregator.class;
    }

    @Override public void write( ObjectDataOutput out, FeatureExtractionAggregator object ) throws IOException {
        GraphEntityPairStreamSerializer.serialize( out, object.getGraphEntityPair() );
        LinkingEntityStreamSerializer.serialize( out, object.getLinkingEntity() );

        out.writeInt( object.getPropertyTypeIdIndexedByFqn().size() );
        for ( Map.Entry<FullQualifiedName, UUID> entry : object.getPropertyTypeIdIndexedByFqn().entrySet() ) {
            FullQualifiedNameStreamSerializer.serialize( out, entry.getKey() );
            UUIDStreamSerializer.serialize( out, entry.getValue() );
        }

        out.writeDouble( object.getLightest() );
    }

    @Override public FeatureExtractionAggregator read( ObjectDataInput in ) throws IOException {
        GraphEntityPair graphEntityPair = GraphEntityPairStreamSerializer.deserialize( in );
        LinkingEntity linkingEntity = LinkingEntityStreamSerializer.deserialize( in );

        Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn = Maps.newHashMap();
        int fqnMapSize = in.readInt();
        for ( int i = 0; i < fqnMapSize; i++ ) {
            FullQualifiedName fqn = FullQualifiedNameStreamSerializer.deserialize( in );
            UUID id = UUIDStreamSerializer.deserialize( in );
            propertyTypeIdIndexedByFqn.put( fqn, id );
        }

        double lightest = in.readDouble();
        return new FeatureExtractionAggregator( graphEntityPair, linkingEntity, propertyTypeIdIndexedByFqn, lightest, elasticsearchApi );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.FEATURE_EXTRACTION_AGGREGATOR.ordinal();
    }

    @Override public void destroy() {

    }


        public synchronized void setConductorElasticsearchApi( ConductorElasticsearchApi api ) {
            Preconditions.checkState( this.elasticsearchApi == null, "Api can only be set once" );
            this.elasticsearchApi = Preconditions.checkNotNull( api );
        }
}
