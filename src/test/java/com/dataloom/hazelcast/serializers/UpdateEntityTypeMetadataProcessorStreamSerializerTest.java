package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.types.processors.UpdateEntityTypeMetadataProcessor;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class UpdateEntityTypeMetadataProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<UpdateEntityTypeMetadataProcessorStreamSerializer, UpdateEntityTypeMetadataProcessor>
        implements Serializable {
    private static final long serialVersionUID = 6256754971472117558L;

    @Override
    protected UpdateEntityTypeMetadataProcessorStreamSerializer createSerializer() {
        return new UpdateEntityTypeMetadataProcessorStreamSerializer();
    }

    @Override
    protected UpdateEntityTypeMetadataProcessor createInput() {
        EntityType et = TestDataFactory.entityType();
        MetadataUpdate update = new MetadataUpdate(
                Optional.of( et.getTitle() ),
                Optional.of( et.getDescription() ),
                Optional.absent(),
                Optional.absent(),
                Optional.of( et.getType() ),
                Optional.absent(),
                Optional.absent() );
        return new UpdateEntityTypeMetadataProcessor( update );
    }

}
