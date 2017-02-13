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

package com.kryptnostic.conductor.rpc;

import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organizations.processors.PrincipalRemover;
import com.google.common.collect.ImmutableSet;
import com.dataloom.hazelcast.serializers.PrincipalRemoverStreamSerializer;
import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class PrincipalRemoverStreamSerializerTest
        extends AbstractStreamSerializerTest<PrincipalRemoverStreamSerializer, PrincipalRemover> {
    @Override protected PrincipalRemoverStreamSerializer createSerializer() {
        return new PrincipalRemoverStreamSerializer();
    }

    @Override protected PrincipalRemover createInput() {
        return new PrincipalRemover( ImmutableSet
                .of( TestDataFactory.userPrincipal(), TestDataFactory.userPrincipal() ) );
    }
}
