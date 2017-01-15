package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.geekbeast.rhizome.tests.bootstrap.CassandraBootstrap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.cassandra.CassandraTablesPod;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.pods.CassandraPod;

public class HzAuthzTest extends CassandraBootstrap {
    private static RhizomeApplicationServer      testServer;
    private static HazelcastInstance             hazelcastInstance;
    private static Session                       session;
    private static AuthorizationQueryService     aqs;
    private static HazelcastAuthorizationService hzAuthz;

    @BeforeClass
    public static void init() {
        testServer = new RhizomeApplicationServer(
                MapstoresPod.class,
                CassandraPod.class,
                TypeCodecsPod.class,
                CassandraTablesPod.class );
        testServer.sprout( "local", CassandraPod.CASSANDRA_PROFILE );
        hazelcastInstance = testServer.getContext().getBean( HazelcastInstance.class );
        session = testServer.getContext().getBean( Session.class );
        aqs = new AuthorizationQueryService( session, hazelcastInstance );
        hzAuthz = new HazelcastAuthorizationService( hazelcastInstance, aqs );

    }

    @Test
    public void testAddEntitySetPermission() {
        AclKeyPathFragment key = new AclKeyPathFragment( SecurableObjectType.EntitySet, UUID.randomUUID() );
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
        AclKeyPathFragment key = new AclKeyPathFragment( SecurableObjectType.EntitySet, UUID.randomUUID() );
        Principal p = new Principal( PrincipalType.USER, "grid|TRON" );
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( key ), ImmutableSet.of( p ), permissions ) );
        hzAuthz.addPermission( ImmutableList.of( key ), p, permissions );
        AclKeyPathFragment badkey = new AclKeyPathFragment( SecurableObjectType.Datasource, UUID.randomUUID() );
        Assert.assertFalse(
                hzAuthz.checkIfHasPermissions( ImmutableList.of( badkey ), ImmutableSet.of( p ), permissions ) );
    }

    @Test
    public void testRemovePermissions() {
        AclKeyPathFragment key = new AclKeyPathFragment( SecurableObjectType.EntitySet, UUID.randomUUID() );
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
        AclKeyPathFragment key = new AclKeyPathFragment( SecurableObjectType.EntitySet, UUID.randomUUID() );
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
