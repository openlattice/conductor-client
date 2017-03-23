package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.organizations.roles.processors.RoleDescriptionUpdater;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RoleDescriptionUpdaterStreamSerializerTest
        extends AbstractStreamSerializerTest<RoleDescriptionUpdaterStreamSerializer, RoleDescriptionUpdater>
        implements Serializable {

    private static final long serialVersionUID = 9031049895181605500L;

    @Override
    protected RoleDescriptionUpdater createInput() {
        return new RoleDescriptionUpdater( RandomStringUtils.randomAlphanumeric( 5 ) );
    }

    @Override
    protected RoleDescriptionUpdaterStreamSerializer createSerializer() {
        return new RoleDescriptionUpdaterStreamSerializer();
    }

}
