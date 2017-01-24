package com.dataloom.authorization;

import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.hazelcast.pods.SharedStreamSerializersPod;
import com.dataloom.mapstores.TestDataFactory;
import com.datastax.driver.core.Session;
import com.geekbeast.rhizome.tests.bootstrap.CassandraBootstrap;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.datastore.cassandra.CassandraTablesPod;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.pods.CassandraPod;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HzAuthzTest extends CassandraBootstrap {
    protected static final RhizomeApplicationServer      testServer;
    protected static final HazelcastInstance             hazelcastInstance;
    protected static final Session                       session;
    protected static final CassandraConfiguration        cc;
    protected static final AuthorizationQueryService     aqs;
    protected static final HazelcastAuthorizationService hzAuthz;
    private static final Logger logger = LoggerFactory.getLogger( HzAuthzTest.class );

    static {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        ;
        testServer = new RhizomeApplicationServer(
                MapstoresPod.class,
                CassandraPod.class,
                TypeCodecsPod.class,
                SharedStreamSerializersPod.class,
                CassandraTablesPod.class );
        testServer.sprout( "local", CassandraPod.CASSANDRA_PROFILE );
        hazelcastInstance = testServer.getContext().getBean( HazelcastInstance.class );
        session = testServer.getContext().getBean( Session.class );
        cc = testServer.getContext().getBean( CassandraConfiguration.class );
        aqs = new AuthorizationQueryService( cc.getKeyspace(), session, hazelcastInstance );
        hzAuthz = new HazelcastAuthorizationService( hazelcastInstance,
                aqs,
                testServer.getContext().getBean( EventBus.class ) );

    }

    @Test
    public void testAddEntitySetPermission() {
        UUID key = UUID.randomUUID();
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.addPermission( ImmutableList.of( key ), p, permissions );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testTypeMistmatchPermission() {
        UUID key = UUID.randomUUID();
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.addPermission( ImmutableList.of( key ), p, permissions );
        UUID badkey = UUID.randomUUID();
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( badkey ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testRemovePermissions() {
        UUID key = UUID.randomUUID();
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.addPermission( ImmutableList.of( key ), p, permissions );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.removePermission( ImmutableList.of( key ), p, permissions );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testSetPermissions() {
        UUID key = UUID.randomUUID();
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        EnumSet<Permission> badPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ, Permission.LINK );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.setPermission( ImmutableList.of( key ), p, permissions );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), badPermissions ) );
        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testListSecurableObjects() {
        ImmutableList key = ImmutableList.of( UUID.randomUUID() );
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
        hzAuthz.createEmptyAcl( key, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key, p2, permissions2 );

        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), permissions1 ) );

        Assert.assertFalse( hzAuthz.checkIfHasPermissions( key,
                ImmutableSet.of( p1 ),
                EnumSet.of( Permission.WRITE, Permission.OWNER ) ) );

        Assert.assertTrue(
                hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p2 ), permissions2 ) );

        Stream<List<UUID>> p1Owned = hzAuthz.getAuthorizedObjectsOfType( ImmutableSet.of( p1 ),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.OWNER ) );

        Set<List<UUID>> p1s = p1Owned.collect( Collectors.toSet() );

        if ( p1s.size() > 0 ) {
            Set<Permission> permissions =  hzAuthz.getSecurableObjectPermissions( key, ImmutableSet.of( p1 ) );
            Assert.assertTrue( permissions.contains( Permission.OWNER) );
            Assert.assertTrue(
                    hzAuthz.checkIfHasPermissions( key, ImmutableSet.of( p1 ), EnumSet.of( Permission.OWNER ) ) );
        }

        Stream<List<UUID>> p2Owned = hzAuthz.getAuthorizedObjectsOfType( ImmutableSet.of( p2 ),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.OWNER ) );

        Set<List<UUID>> p2s = p2Owned.collect( Collectors.toSet() );
        Assert.assertTrue( p1s.isEmpty() );
        Assert.assertEquals( 1, p2s.size() );
        Assert.assertFalse( p1s.contains( key ) );
        Assert.assertTrue( p2s.contains( key ) );
    }


}
