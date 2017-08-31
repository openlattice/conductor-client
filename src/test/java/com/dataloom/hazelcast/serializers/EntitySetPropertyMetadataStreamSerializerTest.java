package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import com.dataloom.edm.set.EntitySetPropertyMetadata;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class EntitySetPropertyMetadataStreamSerializerTest
        extends AbstractStreamSerializerTest<EntitySetPropertyMetadataStreamSerializer, EntitySetPropertyMetadata>
        implements Serializable {
    private static final long serialVersionUID = 5114029297563838101L;

    @Override
    protected EntitySetPropertyMetadataStreamSerializer createSerializer() {
        return new EntitySetPropertyMetadataStreamSerializer();
    }

    @Override
    protected EntitySetPropertyMetadata createInput() {
        return new EntitySetPropertyMetadata( "title", "description", true );
    }

}
