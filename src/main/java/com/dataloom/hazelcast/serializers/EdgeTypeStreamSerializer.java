package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.dataloom.edm.type.EdgeType;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.google.common.base.Optional;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Component
public class EdgeTypeStreamSerializer implements SelfRegisteringStreamSerializer<EdgeType> {

    @Override
    public void write( ObjectDataOutput out, EdgeType object ) throws IOException {
        SetStreamSerializers.serialize( out, object.getSrc(), ( UUID key ) -> {
            UUIDStreamSerializer.serialize( out, key );
        } );
        SetStreamSerializers.serialize( out, object.getDest(), ( UUID property ) -> {
            UUIDStreamSerializer.serialize( out, property );
        } );
        out.writeBoolean( object.isBidirectional() );
    }

    @Override
    public EdgeType read( ObjectDataInput in ) throws IOException {
        LinkedHashSet<UUID> src = SetStreamSerializers.orderedDeserialize( in, UUIDStreamSerializer::deserialize );
        LinkedHashSet<UUID> dest = SetStreamSerializers.orderedDeserialize( in, UUIDStreamSerializer::deserialize );
        boolean bidirectional = in.readBoolean();

        return new EdgeType( Optional.absent(), src, dest, bidirectional );
    }

    @Override
    public int getTypeId() {
        return StreamSerializerTypeIds.EDGE_TYPE.ordinal();
    }

    @Override
    public void destroy() {}

    @Override
    public Class<? extends EdgeType> getClazz() {
        return EdgeType.class;
    }

}
