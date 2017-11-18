package com.dataloom.hazelcast.serializers;

import com.dataloom.apps.App;
import com.google.common.base.Optional;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.util.LinkedHashSet;
import java.util.UUID;

public class AppStreamSerializerTest extends AbstractStreamSerializerTest<AppStreamSerializer, App> {
    @Override protected AppStreamSerializer createSerializer() {
        return new AppStreamSerializer();
    }

    @Override protected App createInput() {
        LinkedHashSet<UUID> configIds = new LinkedHashSet<>();
        configIds.add( UUID.randomUUID() );
        configIds.add( UUID.randomUUID() );
        configIds.add( UUID.randomUUID() );
        return new App( UUID.randomUUID(), "name", "title", Optional.of( "description" ), configIds );
    }
}
