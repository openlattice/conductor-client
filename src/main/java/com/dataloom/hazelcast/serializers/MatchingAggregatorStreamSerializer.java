package com.dataloom.hazelcast.serializers;

import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.dataloom.matching.MatchingAggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Preconditions;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Component
public class MatchingAggregatorStreamSerializer implements SelfRegisteringStreamSerializer<MatchingAggregator> {
    private ConductorElasticsearchApi api;

    @Override
    @SuppressFBWarnings
    public void write( ObjectDataOutput out, MatchingAggregator object ) throws IOException {

        UUIDStreamSerializer.serialize( out, object.getGraphId() );

        out.writeDoubleArray( object.getLightest() );

        out.writeInt( object.getPropertyTypeIdIndexedByFqn().size() );
        for ( Map.Entry<FullQualifiedName, UUID> entry : object.getPropertyTypeIdIndexedByFqn().entrySet() ) {
            FullQualifiedNameStreamSerializer.serialize( out, entry.getKey() );
            UUIDStreamSerializer.serialize( out, entry.getValue() );
        }
    }

    @Override
    @SuppressFBWarnings
    public MatchingAggregator read( ObjectDataInput in ) throws IOException {
        UUID graphId = UUIDStreamSerializer.deserialize( in );
        double[] lightest = in.readDoubleArray();

        Map<FullQualifiedName, UUID> propertyTypeIdIndexedByFqn = Maps.newHashMap();
        int fqnMapSize = in.readInt();
        for ( int i = 0; i < fqnMapSize; i++ ) {
            FullQualifiedName fqn = FullQualifiedNameStreamSerializer.deserialize( in );
            UUID id = UUIDStreamSerializer.deserialize( in );
            propertyTypeIdIndexedByFqn.put( fqn, id );
        }

        return new MatchingAggregator(
                graphId,
                propertyTypeIdIndexedByFqn,
                lightest,
                api );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.MATCHING_AGGREGATOR.ordinal();
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends MatchingAggregator> getClazz() {
        return MatchingAggregator.class;
    }

    public synchronized void setConductorElasticsearchApi( ConductorElasticsearchApi api ) {
        Preconditions.checkState( this.api == null, "Api can only be set once" );
        this.api = Preconditions.checkNotNull( api );
    }

}
