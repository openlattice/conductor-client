package com.dataloom.hazelcast.serializers;

import com.dataloom.organizations.roles.processors.PrincipalDescriptionUpdater;
import java.io.Serializable;

import org.apache.commons.lang3.RandomStringUtils;

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RoleDescriptionUpdaterStreamSerializerTest
        extends AbstractStreamSerializerTest<RoleDescriptionUpdaterStreamSerializer, PrincipalDescriptionUpdater>
        implements Serializable {

    private static final long serialVersionUID = 9031049895181605500L;

    @Override
    protected PrincipalDescriptionUpdater createInput() {
        return new PrincipalDescriptionUpdater( RandomStringUtils.randomAlphanumeric( 5 ) );
    }

    @Override
    protected RoleDescriptionUpdaterStreamSerializer createSerializer() {
        return new RoleDescriptionUpdaterStreamSerializer();
    }

}
