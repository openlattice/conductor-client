package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.dataloom.hazelcast.pods.MapstoresPod;
import com.datastax.driver.core.Session;
import com.geekbeast.rhizome.tests.bootstrap.CassandraBootstrap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.datastore.cassandra.CassandraTablesPod;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.pods.CassandraPod;

public class HzAuthzTest extends CassandraBootstrap {
    protected static final RhizomeApplicationServer      testServer;
    protected static final HazelcastInstance             hazelcastInstance;
    protected static final Session                       session;
    protected static final CassandraConfiguration        cc;
    protected static final AuthorizationQueryService     aqs;
    protected static final HazelcastAuthorizationService hzAuthz;

    static {
        testServer = new RhizomeApplicationServer(
                MapstoresPod.class,
                CassandraPod.class,
                TypeCodecsPod.class,
                CassandraTablesPod.class );
        testServer.sprout( "local", CassandraPod.CASSANDRA_PROFILE );
        hazelcastInstance = testServer.getContext().getBean( HazelcastInstance.class );
        session = testServer.getContext().getBean( Session.class );
        cc = testServer.getContext().getBean( CassandraConfiguration.class );
        aqs = new AuthorizationQueryService( cc.getKeyspace(), session, hazelcastInstance );
        hzAuthz = new HazelcastAuthorizationService( hazelcastInstance, aqs );

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
}
