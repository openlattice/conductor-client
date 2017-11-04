package com.dataloom.hazelcast.serializers;

import com.dataloom.apps.AppConfigKey;
import com.dataloom.hazelcast.StreamSerializerTypeIds;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
public class AppConfigKeyStreamSerializer implements SelfRegisteringStreamSerializer<AppConfigKey> {
    @Override public Class<? extends AppConfigKey> getClazz() {
        return AppConfigKey.class;
    }

    @Override public void write( ObjectDataOutput out, AppConfigKey object ) throws IOException {
        UUIDStreamSerializer.serialize( out, object.getAppId() );
        UUIDStreamSerializer.serialize( out, object.getOrganizationId() );
        UUIDStreamSerializer.serialize( out, object.getAppTypeId() );
    }

    @Override public AppConfigKey read( ObjectDataInput in ) throws IOException {
        UUID appId = UUIDStreamSerializer.deserialize( in );
        UUID organizationId = UUIDStreamSerializer.deserialize( in );
        UUID appTypeId = UUIDStreamSerializer.deserialize( in );
        return new AppConfigKey( appId, organizationId, appTypeId );
    }

    @Override public int getTypeId() {
        return StreamSerializerTypeIds.APP_CONFIG_KEY.ordinal();
    }

    @Override public void destroy() {

    }
}
