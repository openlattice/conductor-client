package com.dataloom.hazelcast.serializers;

import com.dataloom.graph.core.objects.NeighborTripletSet;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.collect.Sets;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.ListStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.openlattice.rhizome.hazelcast.DelegatedUUIDList;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class NeighborTripletSetStreamSerializer implements SelfRegisteringStreamSerializer<NeighborTripletSet> {
    @Override public Class<? extends NeighborTripletSet> getClazz() {
        return NeighborTripletSet.class;
    }

    @Override public void write( ObjectDataOutput out, NeighborTripletSet object ) throws IOException {
        serialize( out, object );
    }

    @Override public NeighborTripletSet read( ObjectDataInput in ) throws IOException {
        return deserialize( in );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.NEIGHBOR_TRIPLET_SET.ordinal();
    }

    @Override public void destroy() {

    }

    public static void serialize( ObjectDataOutput out, NeighborTripletSet object ) throws IOException {
        out.writeInt( object.size() );
        for ( DelegatedUUIDList triplet : object ) {
            UUIDStreamSerializer.serialize( out, triplet.get( 0 ) );
            UUIDStreamSerializer.serialize( out, triplet.get( 1 ) );
            UUIDStreamSerializer.serialize( out, triplet.get( 2 ) );
        }
    }

    public static NeighborTripletSet deserialize( ObjectDataInput in ) throws IOException {
        NeighborTripletSet result = new NeighborTripletSet( Sets.newHashSet() );
        int size = in.readInt();
        for ( int i = 0; i < size; i++ ) {
            UUID src = UUIDStreamSerializer.deserialize( in );
            UUID assoc = UUIDStreamSerializer.deserialize( in );
            UUID dst = UUIDStreamSerializer.deserialize( in );
            result.add( new DelegatedUUIDList( src, assoc, dst ) );
        }
        return result;
    }
}
