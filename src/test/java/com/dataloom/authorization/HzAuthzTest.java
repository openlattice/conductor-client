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

package com.dataloom.authorization;

import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.hazelcast.pods.SharedStreamSerializersPod;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.neuron.Neuron;
import com.dataloom.neuron.pods.NeuronPod;
import com.datastax.driver.core.Session;
import com.geekbeast.rhizome.tests.bootstrap.CassandraBootstrap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.openlattice.authorization.AclKey;
import com.openlattice.jdbc.JdbcPod;
import com.openlattice.postgres.PostgresPod;
import com.openlattice.postgres.PostgresTablesPod;
import com.zaxxer.hikari.HikariDataSource;
import digital.loom.rhizome.authentication.Auth0Pod;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HzAuthzTest {
    protected static final RhizomeApplicationServer      testServer;
    protected static final HazelcastInstance             hazelcastInstance;
    protected static final AuthorizationQueryService     aqs;
    protected static final HazelcastAuthorizationService hzAuthz;
    protected static final Neuron                        neuron;
    protected static final HikariDataSource              hds;
    private static final Logger logger = LoggerFactory.getLogger( HzAuthzTest.class );

    static {
        testServer = new RhizomeApplicationServer(
                Auth0Pod.class,
                MapstoresPod.class,
                JdbcPod.class,
                PostgresPod.class,
                TypeCodecsPod.class,
                SharedStreamSerializersPod.class,
                PostgresTablesPod.class,
                NeuronPod.class
        );

        testServer.sprout( ConfigurationConstants.Profiles.LOCAL_CONFIGURATION_PROFILE, PostgresPod.PROFILE );
        hazelcastInstance = testServer.getContext().getBean( HazelcastInstance.class );

        neuron = testServer.getContext().getBean( Neuron.class );
        hds = testServer.getContext().getBean( HikariDataSource.class );

        aqs = new AuthorizationQueryService( hds, hazelcastInstance );
        hzAuthz = new HazelcastAuthorizationService(
                hazelcastInstance,
                aqs,
                testServer.getContext().getBean( EventBus.class )
        );
    }

    @Test
    public void testAddEntitySetPermission() {
        UUID key = UUID.randomUUID();
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.addPermission( new AclKey( key ), p, permissions );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testTypeMistmatchPermission() {
        UUID key = UUID.randomUUID();
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.addPermission( new AclKey( key ), p, permissions );
        UUID badkey = UUID.randomUUID();
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( new AclKey( badkey ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testRemovePermissions() {
        UUID key = UUID.randomUUID();
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.addPermission( new AclKey( key ), p, permissions );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.removePermission( new AclKey( key ), p, permissions );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testSetPermissions() {
        UUID key = UUID.randomUUID();
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        EnumSet<Permission> badPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ, Permission.LINK );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.setPermission( new AclKey( key ), p, permissions );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), badPermissions ) );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( new AclKey( key ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testListSecurableObjects() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p1 = TestDataFactory.userPrincipal();
        Principal p2 = TestDataFactory.userPrincipal();

        EnumSet<Permission> permissions1 = EnumSet.of( Permission.DISCOVER, Permission.READ );
        EnumSet<Permission> permissions2 = EnumSet
                .of( Permission.DISCOVER, Permission.READ, Permission.WRITE, Permission.OWNER );

        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );

        hzAuthz.addPermission( key, p1, permissions1 );
        hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key, p2, permissions2 );

        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );

        Assert.assertFalse( hzAuthz.checkIfHasPermissions( key,
                ImmutableSet.of( p1 ),
                EnumSet.of( Permission.WRITE, Permission.OWNER ) ) );

        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );

        Stream<AclKey> p1Owned = hzAuthz.getAuthorizedObjectsOfType( ImmutableSet.of( p1 ),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.OWNER ) );

        Set<List<UUID>> p1s = p1Owned.collect( Collectors.toSet() );

        if ( p1s.size() > 0 ) {
            Set<Permission> permissions = hzAuthz.getSecurableObjectPermissions( key, ImmutableSet.of( p1 ) );
            Assert.assertTrue( permissions.contains( Permission.OWNER ) );
            Assert.assertTrue(
                    hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), EnumSet.of( Permission.OWNER ) ) );
        }

        Stream<AclKey> p2Owned = hzAuthz.getAuthorizedObjectsOfType( ImmutableSet.of( p2 ),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.OWNER ) );

        Set<List<UUID>> p2s = p2Owned.collect( Collectors.toSet() );
        Assert.assertTrue( p1s.isEmpty() );
        Assert.assertEquals( 1, p2s.size() );
        Assert.assertFalse( p1s.contains( key ) );
        Assert.assertTrue( p2s.contains( key ) );
    }

    @Test
    public void testAccessChecks() {
        AclKey key = new AclKey( UUID.randomUUID() );
        Principal p1 = TestDataFactory.userPrincipal();
        Principal p2 = TestDataFactory.userPrincipal();

        EnumSet<Permission> permissions1 = EnumSet.of( Permission.DISCOVER, Permission.READ, Permission.OWNER );
        EnumSet<Permission> permissions2 = EnumSet
                .of( Permission.DISCOVER, Permission.READ, Permission.WRITE, Permission.LINK  );

        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );

        hzAuthz.addPermission( key, p1, permissions1 );
        hzAuthz.setSecurableObjectType( key, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key, p2, permissions2 );

        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );

        Assert.assertFalse( hzAuthz.checkIfHasPermissions( key,
                ImmutableSet.of( p1 ),
                EnumSet.of( Permission.WRITE, Permission.OWNER ) ) );

        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );

        AccessCheck ac = new AccessCheck( key, permissions1 );
        Map<AclKey, EnumMap<Permission, Boolean>> result =
                hzAuthz.accessChecksForPrincipals( ImmutableSet.of( ac ), ImmutableSet.of( p2 ) );

        Assert.assertTrue( result.containsKey( key ) );
        EnumMap<Permission,Boolean> checkForKey = result.get( key );
        Assert.assertTrue( checkForKey.size() == permissions1.size() );

        Assert.assertTrue( result.get( key ).get( Permission.DISCOVER )  );
        Assert.assertTrue( result.get( key ).get( Permission.READ )  );
        Assert.assertFalse( result.get( key ).get( Permission.OWNER )  );
    }

}
