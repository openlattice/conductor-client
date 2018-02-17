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

package com.dataloom.mapstores;

import com.openlattice.authorization.AceKey;
import com.dataloom.authorization.HzAuthzTest;
import com.openlattice.authorization.securable.AbstractSecurableObject;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.openlattice.authorization.AceValue;
import com.openlattice.authorization.mapstores.PostgresCredentialMapstore;
import com.openlattice.authorization.mapstores.UserMapstore;
import com.openlattice.postgres.mapstores.SyncIdsMapstore;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MapstoresTest extends HzAuthzTest {
    private static final Logger      logger   = LoggerFactory
            .getLogger( MapstoresTest.class );
    private static final Set<String> excluded =
            ImmutableSet.of( HazelcastMap.EDGES.name(),
                    HazelcastMap.BACKEDGES.name(),
                    HazelcastMap.PERMISSIONS.name() );
    @SuppressWarnings( "rawtypes" )
    private static final Map<String, TestableSelfRegisteringMapStore> mapstoreMap;
    private static final Collection<TestableSelfRegisteringMapStore>  mapstores;

    static {
        mapstores = testServer.getContext().getBeansOfType( TestableSelfRegisteringMapStore.class ).values();
        mapstoreMap = mapstores.stream().collect( Collectors.toMap( TestableSelfRegisteringMapStore::getMapName,
                Function.identity() ) );
    }

    @Test
    public void testPermissionMapstore() {
        TestableSelfRegisteringMapStore permissions = mapstoreMap.get( HazelcastMap.PERMISSIONS.name() );
        TestableSelfRegisteringMapStore objectTypes = mapstoreMap.get( HazelcastMap.SECURABLE_OBJECT_TYPES.name() );

        AceValue expected = (AceValue) permissions.generateTestValue();
        AceKey key = (AceKey) permissions.generateTestKey();

        Object actual = null;
        try {
            objectTypes.store( key.getAclKey(), expected.getSecurableObjectType() );
            permissions.store( key, expected );
            actual = permissions.load( key );
            if ( !expected.equals( actual ) ) {
                logger.error( "Incorrect r/w to mapstore {} for key {}. expected({}) != actual({})",
                        permissions.getMapName(),
                        key,
                        expected,
                        actual );
            }
            Assert.assertEquals( expected, actual );
        } catch ( NotImplementedException | UnsupportedOperationException e ) {
            logger.info( "Mapstore not implemented." );
        } catch ( Exception e ) {
            logger.error( "Unable to r/w to mapstore {} value: ({},{})", permissions.getMapName(), key, expected, e );
            throw e;
        }
    }

    @Test
    public void testMapstore() {
        mapstores.stream()
                .filter( ms -> !excluded.contains( ms.getMapName() ) )
                .forEach( MapstoresTest::test );
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    private static void test( TestableSelfRegisteringMapStore ms ) {
        if ( ms instanceof SyncIdsMapstore || ms instanceof PostgresCredentialMapstore || ms instanceof UserMapstore ) {
            return;
        }
        Object expected = ms.generateTestValue();
        Object key = ms.generateTestKey();
        if ( AbstractSecurableObject.class.isAssignableFrom( expected.getClass() )
                && UUID.class.equals( key.getClass() ) ) {
            key = ( (AbstractSecurableObject) expected ).getId();
        }
        Object actual = null;
        try {
            ms.store( key, expected );
            actual = ms.load( key );
            if ( !expected.equals( actual ) ) {
                logger.error( "Incorrect r/w to mapstore {} for key {}. expected({}) != actual({})",
                        ms.getMapName(),
                        key,
                        expected,
                        actual );
            }
            Assert.assertEquals( expected, actual );
        } catch ( NotImplementedException | UnsupportedOperationException e ) {
            logger.info( "Mapstore not implemented." );
        } catch ( Exception e ) {
            logger.error( "Unable to r/w to mapstore {} value: ({},{})", ms.getMapName(), key, expected, e );
            throw e;
        }
    }
}
