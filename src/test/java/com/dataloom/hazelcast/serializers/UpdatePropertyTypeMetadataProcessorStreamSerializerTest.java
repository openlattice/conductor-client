package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.edm.types.processors.UpdatePropertyTypeMetadataProcessor;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class UpdatePropertyTypeMetadataProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<UpdatePropertyTypeMetadataProcessorStreamSerializer, UpdatePropertyTypeMetadataProcessor>
        implements Serializable {
    private static final long serialVersionUID = 256336851860597599L;

    @Override
    protected UpdatePropertyTypeMetadataProcessorStreamSerializer createSerializer() {
        return new UpdatePropertyTypeMetadataProcessorStreamSerializer();
    }

    @Override
    protected UpdatePropertyTypeMetadataProcessor createInput() {
        PropertyType pt = TestDataFactory.propertyType();
        MetadataUpdate update = new MetadataUpdate(
                Optional.of( pt.getTitle() ),
                Optional.of( pt.getDescription() ),
                Optional.absent(),
                Optional.absent(),
                Optional.of( pt.getType() ),
                Optional.absent(),
                Optional.absent(),
                Optional.absent() );
        return new UpdatePropertyTypeMetadataProcessor( update );
    }

}
