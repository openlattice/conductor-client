package com.dataloom.hazelcast.serializers;

import com.dataloom.graph.core.objects.EdgeCountEntryProcessor;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractUUIDStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.SetStreamSerializers;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@Component
public class EdgeCountEntryProcessorStreamSerializer
        implements SelfRegisteringStreamSerializer<EdgeCountEntryProcessor> {
    @Override public Class<EdgeCountEntryProcessor> getClazz() {
        return EdgeCountEntryProcessor.class;
    }

    @Override public void write(
            ObjectDataOutput out, EdgeCountEntryProcessor object ) throws IOException {
        AbstractUUIDStreamSerializer.serialize( out, object.getAssociationTypeId() );
        SetStreamSerializers.fastUUIDSetSerialize( out, object.getNeighborTypeIds() );

    }

    @Override public EdgeCountEntryProcessor read( ObjectDataInput in ) throws IOException {
        UUID associationTypeId = AbstractUUIDStreamSerializer.deserialize( in );
        Set<UUID> neighorhoodTypeIds = SetStreamSerializers.fastUUIDSetDeserialize( in );

        return new EdgeCountEntryProcessor( associationTypeId, neighorhoodTypeIds );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.EDGE_COUNT_ENTRY_PROCESSOR.ordinal();
    }

    @Override public void destroy() {

    }
}
