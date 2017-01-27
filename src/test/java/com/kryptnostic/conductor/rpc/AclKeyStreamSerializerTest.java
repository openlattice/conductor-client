package com.kryptnostic.conductor.rpc;

import com.dataloom.authorization.AclKey;
import com.dataloom.mapstores.TestDataFactory;
import com.kryptnostic.conductor.rpc.serializers.AclKeyStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDList;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class AclKeyStreamSerializerTest extends
        AbstractStreamSerializerTest<AclKeyStreamSerializer, DelegatedUUIDList> {

    @Override
    protected AclKeyStreamSerializer createSerializer() {
        return new AclKeyStreamSerializer();
    }

    @Override
    protected AclKey createInput() {
        return AclKey.wrap( TestDataFactory.aclKey() );
    }
}
