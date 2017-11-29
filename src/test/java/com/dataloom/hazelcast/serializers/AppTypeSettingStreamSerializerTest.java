package com.dataloom.hazelcast.serializers;

import com.dataloom.apps.AppTypeSetting;
import com.dataloom.authorization.Permission;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

import java.util.EnumSet;
import java.util.UUID;

public class AppTypeSettingStreamSerializerTest
        extends AbstractStreamSerializerTest<AppTypeSettingStreamSerializer, AppTypeSetting> {
    @Override protected AppTypeSettingStreamSerializer createSerializer() {
        return new AppTypeSettingStreamSerializer();
    }

    @Override protected AppTypeSetting createInput() {
        return new AppTypeSetting( UUID.randomUUID(), EnumSet.of( Permission.READ, Permission.WRITE ) );
    }
}
