package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.edm.EntitySet;
import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.types.processors.UpdateEntitySetMetadataProcessor;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class UpdateEntitySetMetadataProcessorStreamSerializerTest extends
        AbstractStreamSerializerTest<UpdateEntitySetMetadataProcessorStreamSerializer, UpdateEntitySetMetadataProcessor>
        implements Serializable {

    private static final long serialVersionUID = -1157333335898067222L;

    @Override
    protected UpdateEntitySetMetadataProcessorStreamSerializer createSerializer() {
        return new UpdateEntitySetMetadataProcessorStreamSerializer();
    }

    @Override
    protected UpdateEntitySetMetadataProcessor createInput() {
        EntitySet es = TestDataFactory.entitySet();
        MetadataUpdate update = new MetadataUpdate(
                Optional.of( es.getTitle() ),
                Optional.of( es.getDescription() ),
                Optional.of( es.getName() ),
                Optional.of( es.getContacts() ),
                Optional.absent(),
                Optional.absent(),
                Optional.absent() );
        return new UpdateEntitySetMetadataProcessor( update );
    }

}