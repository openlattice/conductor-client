package com.dataloom.hazelcast.serializers;

import com.dataloom.organizations.roles.processors.PrincipalTitleUpdater;
import java.io.Serializable;

import org.apache.commons.lang3.RandomStringUtils;

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RoleTitleUpdaterStreamSerializerTest
        extends AbstractStreamSerializerTest<RoleTitleUpdaterStreamSerializer, PrincipalTitleUpdater>
        implements Serializable {
    private static final long serialVersionUID = 3284719591100074926L;

    @Override
    protected PrincipalTitleUpdater createInput() {
        return new PrincipalTitleUpdater( RandomStringUtils.randomAlphanumeric( 5 ) );
    }

    @Override
    protected RoleTitleUpdaterStreamSerializer createSerializer() {
        return new RoleTitleUpdaterStreamSerializer();
    }

}
