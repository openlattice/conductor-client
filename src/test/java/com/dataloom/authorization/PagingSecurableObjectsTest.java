package com.dataloom.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.paging.AuthorizedObjectsPagingFactory;
import com.dataloom.authorization.paging.AuthorizedObjectsSearchResult;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.collect.ImmutableList;

public class PagingSecurableObjectsTest extends HzAuthzTest {
    private static final Logger                     logger = LoggerFactory.getLogger( PagingSecurableObjectsTest.class );

    // Entity Set acl Keys
    protected static List<UUID>              key1 = ImmutableList.of( UUID.randomUUID() );
    protected static List<UUID>              key2 = ImmutableList.of( UUID.randomUUID() );
    protected static List<UUID>              key3 = ImmutableList.of( UUID.randomUUID() );

    // User and roles
    protected static final Principal               u1 = TestDataFactory.userPrincipal();
    protected static final Principal               r1 = TestDataFactory.rolePrincipal();
    protected static final Principal               r2 = TestDataFactory.rolePrincipal();
    protected static final Principal               r3 = TestDataFactory.rolePrincipal();
    protected static final NavigableSet<Principal> currentPrincipals = new TreeSet<Principal>();

    @BeforeClass
    public static void init() {
        currentPrincipals.add( u1 );
        currentPrincipals.add( r1 );
        currentPrincipals.add( r2 );
        currentPrincipals.add( r3 );

        hzAuthz.addPermission( key1, u1, EnumSet.allOf( Permission.class ) );
        hzAuthz.createEmptyAcl( key1, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key2, r1, EnumSet.of( Permission.READ, Permission.WRITE ) );
        hzAuthz.createEmptyAcl( key2, SecurableObjectType.EntitySet );
        hzAuthz.addPermission( key3, r2, EnumSet.of( Permission.READ ) );
        hzAuthz.createEmptyAcl( key3, SecurableObjectType.EntitySet );
    }

    @Test
    public void testListSecurableObjectsInOnePage() {
        int LARGE_PAGE_SIZE = 20;
        AuthorizedObjectsSearchResult result = hzAuthz.getAuthorizedObjectsOfType( currentPrincipals,
                SecurableObjectType.EntitySet,
                Permission.READ,
                null,
                LARGE_PAGE_SIZE );

        Assert.assertNull( result.getPagingToken() );
        Assert.assertEquals( 3, result.getAuthorizedObjects().size() );
    }

    @Test
    public void testListSecurableObjectsInMultiplePages() {
        //There should be 3 results in total.
        int SMALL_PAGE_SIZE = 1;

        AuthorizedObjectsSearchResult result = hzAuthz.getAuthorizedObjectsOfType( currentPrincipals,
                SecurableObjectType.EntitySet,
                Permission.READ,
                null,
                SMALL_PAGE_SIZE );

        logger.debug( "First page has result: " + result);
        //First page should have 1 result.
        Assert.assertNotNull( result.getPagingToken() );
        Assert.assertEquals( 1, result.getAuthorizedObjects().size() );

        result = hzAuthz.getAuthorizedObjectsOfType( currentPrincipals,
                SecurableObjectType.EntitySet,
                Permission.READ,
                AuthorizedObjectsPagingFactory.decode( result.getPagingToken() ),
                SMALL_PAGE_SIZE );

        logger.debug( "Second page has result: " + result);
        //Second page should have 1 result.
        Assert.assertNotNull( result.getPagingToken() );
        Assert.assertEquals( 1, result.getAuthorizedObjects().size() );


        result = hzAuthz.getAuthorizedObjectsOfType( currentPrincipals,
                SecurableObjectType.EntitySet,
                Permission.READ,
                AuthorizedObjectsPagingFactory.decode( result.getPagingToken() ),
                SMALL_PAGE_SIZE );

        logger.debug( "Third page has result: " + result);
        //Third page should have 1 result.
        try { 
            Assert.assertNull( result.getPagingToken() );
        } catch ( AssertionError e ){
            result = hzAuthz.getAuthorizedObjectsOfType( currentPrincipals,
                    SecurableObjectType.EntitySet,
                    Permission.READ,
                    AuthorizedObjectsPagingFactory.decode( result.getPagingToken() ),
                    SMALL_PAGE_SIZE );
            logger.debug( "Paging token should be null but it is not: next page using the token gives " + result );
            throw e;
        }
        Assert.assertEquals( 1, result.getAuthorizedObjects().size() );
}

    @Test
    public void testNoResults() {
        int SMALL_PAGE_SIZE = 1;

        AuthorizedObjectsSearchResult result = hzAuthz.getAuthorizedObjectsOfType( currentPrincipals,
                SecurableObjectType.Organization,
                Permission.READ,
                null,
                SMALL_PAGE_SIZE );

        Assert.assertNull( result.getPagingToken() );
        Assert.assertEquals( 0, result.getAuthorizedObjects().size() );
    }

}
