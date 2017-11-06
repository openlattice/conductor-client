package com.dataloom.hazelcast.serializers;

import com.dataloom.apps.AppType;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.UUID;

public class AppTypeStreamSerializerTest extends AbstractStreamSerializerTest<AppTypeStreamSerializer, AppType> {
    @Override protected AppTypeStreamSerializer createSerializer() {
        return new AppTypeStreamSerializer();
    }

    @Override protected AppType createInput() {
        return new AppType( UUID.randomUUID(),
                new FullQualifiedName( "namespace.name" ),
                "title",
                Optional.of( "description" ),
                UUID.randomUUID() );
    }
}
