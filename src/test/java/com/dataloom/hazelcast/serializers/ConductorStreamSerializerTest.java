/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.hazelcast.serializers;

import java.io.IOException;
import java.io.Serializable;

import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.dataloom.hazelcast.serializers.ConductorCallStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

@SuppressWarnings( "rawtypes" )
public class ConductorStreamSerializerTest extends AbstractStreamSerializerTest<ConductorCallStreamSerializer, ConductorCall>
        implements Serializable {
    private static final long serialVersionUID = -8844481298074343953L;

    @Override
    protected ConductorCallStreamSerializer createSerializer() {
        return new ConductorCallStreamSerializer();
    }

    @Override
    protected ConductorCall createInput() {
        return ConductorCall
                .wrap( Lambdas.getAllEntitiesOfType( new FullQualifiedName( "abc", "def" ), ImmutableList.of() ) );
    }

    @Override
    @Test(
        expected = AssertionError.class )
    public void testSerializeDeserialize() throws SecurityException, IOException {
        super.testSerializeDeserialize();
    }
}
