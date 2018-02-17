package com.dataloom.hazelcast.serializers;

import com.openlattice.apps.AppConfigKey;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.util.UUID;

public class AppConfigKeyStreamSerializerTest
        extends AbstractStreamSerializerTest<AppConfigKeyStreamSerializer, AppConfigKey> {
    @Override protected AppConfigKeyStreamSerializer createSerializer() {
        return new AppConfigKeyStreamSerializer();
    }

    @Override protected AppConfigKey createInput() {
        return new AppConfigKey( UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID() );
    }
}
