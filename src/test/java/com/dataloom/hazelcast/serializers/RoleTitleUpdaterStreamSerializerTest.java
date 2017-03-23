package com.dataloom.hazelcast.serializers;

import java.io.Serializable;

import org.apache.commons.lang3.RandomStringUtils;

import com.dataloom.organizations.roles.processors.RoleTitleUpdater;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

public class RoleTitleUpdaterStreamSerializerTest
        extends AbstractStreamSerializerTest<RoleTitleUpdaterStreamSerializer, RoleTitleUpdater>
        implements Serializable {
    private static final long serialVersionUID = 3284719591100074926L;

    @Override
    protected RoleTitleUpdater createInput() {
        return new RoleTitleUpdater( RandomStringUtils.randomAlphanumeric( 5 ) );
    }

    @Override
    protected RoleTitleUpdaterStreamSerializer createSerializer() {
        return new RoleTitleUpdaterStreamSerializer();
    }

}
