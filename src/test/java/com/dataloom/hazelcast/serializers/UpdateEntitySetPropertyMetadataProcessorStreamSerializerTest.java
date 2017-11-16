package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.edm.EntitySet;
import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.types.processors.UpdateEntitySetPropertyMetadataProcessor;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class UpdateEntitySetPropertyMetadataProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<UpdateEntitySetPropertyMetadataProcessorStreamSerializer, UpdateEntitySetPropertyMetadataProcessor>
        implements Serializable {
    private static final long serialVersionUID = -5379472664347656668L;

    @Override
    protected UpdateEntitySetPropertyMetadataProcessorStreamSerializer createSerializer() {
        return new UpdateEntitySetPropertyMetadataProcessorStreamSerializer();
    }

    @Override
    protected UpdateEntitySetPropertyMetadataProcessor createInput() {
        MetadataUpdate update = new MetadataUpdate(
                Optional.of( "title" ),
                Optional.of( "description" ),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.absent(),
                Optional.of( true ) );
        return new UpdateEntitySetPropertyMetadataProcessor( update );
    }

}
