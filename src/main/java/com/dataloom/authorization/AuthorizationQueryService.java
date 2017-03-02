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

import com.dataloom.authorization.paging.AuthorizedObjectsPagingFactory;
import com.dataloom.authorization.paging.AuthorizedObjectsPagingInfo;
import com.dataloom.authorization.paging.AuthorizedObjectsSearchResult;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Iterables;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AuthorizationQueryService {
    private static final Logger                            logger = LoggerFactory
            .getLogger( AuthorizationQueryService.class );
    private final Session                                  session;
    private final PreparedStatement                        authorizedAclKeysQuery;
    private final PreparedStatement                        authorizedAclKeysForObjectTypeQuery;
    private final PreparedStatement                        aclsForSecurableObjectQuery;
    private final PreparedStatement                        ownersForSecurableObjectQuery;
    private final PreparedStatement                        deletePermissionsByAclKeysQuery;
    private final PreparedStatement                        setObjectType;
    private final IMap<AceKey, DelegatedPermissionEnumSet> aces;

    public AuthorizationQueryService( String keyspace, Session session, HazelcastInstance hazelcastInstance ) {
        this.session = session;
        aces = hazelcastInstance.getMap( HazelcastMap.PERMISSIONS.name() );
        authorizedAclKeysQuery = session.prepare( QueryBuilder
                .select( CommonColumns.ACL_KEYS.cql() )
                .from( keyspace, Table.PERMISSIONS.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(),
                        CommonColumns.PRINCIPAL_TYPE.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(),
                        CommonColumns.PRINCIPAL_ID.bindMarker() ) )
                .and( QueryBuilder.contains( CommonColumns.PERMISSIONS.cql(),
                        CommonColumns.PERMISSIONS.bindMarker() ) ) );

        authorizedAclKeysForObjectTypeQuery = session.prepare( QueryBuilder
                .select( CommonColumns.ACL_KEYS.cql() )
                .from( keyspace, Table.PERMISSIONS.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(),
                        CommonColumns.PRINCIPAL_TYPE.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(),
                        CommonColumns.PRINCIPAL_ID.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.SECURABLE_OBJECT_TYPE.cql(),
                        CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) )
                .and( QueryBuilder.contains( CommonColumns.PERMISSIONS.cql(),
                        CommonColumns.PERMISSIONS.bindMarker() ) ) );

        aclsForSecurableObjectQuery = session.prepare( QueryBuilder
                .select( CommonColumns.PRINCIPAL_TYPE.cql(), CommonColumns.PRINCIPAL_ID.cql() )
                .from( keyspace, Table.PERMISSIONS.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(),
                        CommonColumns.ACL_KEYS.bindMarker() ) ) );

        ownersForSecurableObjectQuery = session.prepare( QueryBuilder
                .select( CommonColumns.PRINCIPAL_TYPE.cql(), CommonColumns.PRINCIPAL_ID.cql() )
                .from( keyspace, Table.PERMISSIONS.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(),
                        CommonColumns.ACL_KEYS.bindMarker() ) )
                .and( QueryBuilder.contains( CommonColumns.PERMISSIONS.cql(), Permission.OWNER ) ) );

        deletePermissionsByAclKeysQuery = session
                .prepare( QueryBuilder.delete().from( keyspace, Table.PERMISSIONS.getName() ).where(
                        QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(), CommonColumns.ACL_KEYS.bindMarker() ) ) );

        setObjectType = session.prepare( QueryBuilder
                .update( keyspace, Table.PERMISSIONS.getName() )
                .with( QueryBuilder.set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(),
                        CommonColumns.SECURABLE_OBJECT_TYPE.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(), CommonColumns.ACL_KEYS.bindMarker() ) ) );

    }

    private Stream<List<UUID>> getAuthorizedAclKeys(
            Function<Permission, BoundStatement> binder,
            EnumSet<Permission> desiredPermissions
            ){
        return desiredPermissions
                .stream()
                .map( desiredPermission -> binder.apply( desiredPermission ) )
                .map( session::executeAsync )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( AuthorizationUtils::aclKey );
    }
    
    /**
     * get all authorized acl keys for a principal, of a fixed object type, with desired permissions.
     * @param principal
     * @param objectType
     * @param desiredPermissions
     * @return
     */
    public Stream<List<UUID>> getAuthorizedAclKeys(
            Principal principal,
            SecurableObjectType objectType,
            EnumSet<Permission> desiredPermissions ) {
        return getAuthorizedAclKeys( desiredPermission -> bindAuthorizedAclKeysForObjectTypeQuery( principal,
                objectType,
                desiredPermission ),
                desiredPermissions );
    }

    /**
     * get all authorized acl keys for a set of principals, of a fixed object type, with desired permissions.
     * @param principals
     * @param objectType
     * @param desiredPermissions
     * @return
     */
    public Set<List<UUID>> getAuthorizedAclKeys(
            Set<Principal> principals,
            SecurableObjectType objectType,
            EnumSet<Permission> desiredPermissions ) {
        return principals
                .stream()
                .flatMap( principal -> getAuthorizedAclKeys(
                        principal,
                        objectType,
                        desiredPermissions ) )
                .collect( Collectors.toSet() );
    }

    /**
     * get all authorized acl keys for a set of principals, of a fixed object type, with specified permission, starting from a page given from paging state.
     */
    public AuthorizedObjectsSearchResult getAuthorizedAclKeys(
            NavigableSet<Principal> principals,
            SecurableObjectType objectType,
            Permission permission,
            AuthorizedObjectsPagingInfo pagingInfo,
            int pageSize
            ) {
        Set<List<UUID>> results = new HashSet<>();
        int currentFetchSize = pageSize;
        Principal currentPrincipal = ( pagingInfo == null ) ? principals.first() : pagingInfo.getPrincipal();
        PagingState currentPagingState = ( pagingInfo == null ) ? null : pagingInfo.getPagingState();
        boolean exhausted = false;
        
        do {
            Statement query = bindAuthorizedAclKeysForObjectTypeQuery( currentPrincipal, objectType, permission ).setFetchSize( currentFetchSize );
            
            if( currentPagingState != null ){
                query.setPagingState( currentPagingState );
            }
            
            ResultSet rs = session.execute( query );
            
            int remaining = rs.getAvailableWithoutFetching();
            for( Row row : rs ){
                if( results.add( AuthorizationUtils.aclKey( row ) ) ){
                    currentFetchSize--;
                }
                
                if( --remaining == 0 ){
                    break;
                }
            }
            
            currentPagingState = rs.getExecutionInfo().getPagingState();
            if( currentPagingState == null || rs.isExhausted() ){
                currentPrincipal = principals.higher( currentPrincipal );
                currentPagingState = null;
                
                if( currentPrincipal == null ){
                    exhausted = true;
                }
            }
        } while ( currentFetchSize > 0 && !exhausted );
        
        //When all needed results are fetched, traverse to the next principal so that the next query returns nonzero results.
        while( currentFetchSize == 0 && currentPagingState == null && !exhausted ){
            Statement query = bindAuthorizedAclKeysForObjectTypeQuery( currentPrincipal, objectType, permission ).setFetchSize( 1 );
            ResultSet rs = session.execute( query );
            if( rs.isExhausted() ){
                currentPrincipal = principals.higher( currentPrincipal );
                if( currentPrincipal == null ){
                    exhausted = true;
                    break;
                }
            } else {
                break;
            }
        }
        
        AuthorizedObjectsPagingInfo newPagingInfo = exhausted ? null : AuthorizedObjectsPagingFactory.createSafely( currentPrincipal, currentPagingState );
        String pagingToken = AuthorizedObjectsPagingFactory.encode( newPagingInfo );
        
        return new AuthorizedObjectsSearchResult( pagingToken, results );
    }

    /**
     * get all authorized acl keys for a principal, of all object types, with desired permissions.
     * @param principal
     * @param desiredPermissions
     * @return
     */
    public Stream<List<UUID>> getAuthorizedAclKeys(
            Principal principal,
            EnumSet<Permission> desiredPermissions ) {
        return getAuthorizedAclKeys( desiredPermission -> bindAuthorizedAclKeysQuery( principal, desiredPermission ),
                desiredPermissions );
    }

    /**
     * get all authorized acl keys for a set of principals, of all object types, with desired permissions.
     * @param principals
     * @param desiredPermissions
     * @return
     */
    public Set<List<UUID>> getAuthorizedAclKeys(
            Set<Principal> principals,
            EnumSet<Permission> desiredPermissions ) {
        return principals
                .stream()
                .flatMap( principal -> getAuthorizedAclKeys(
                        principal,
                        desiredPermissions ) )
                .collect( Collectors.toSet() );
    }

    private BoundStatement bindAuthorizedAclKeysForObjectTypeQuery(
            Principal principal,
            SecurableObjectType objectType,
            Permission permission ) {
        return authorizedAclKeysForObjectTypeQuery.bind()
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), principal.getId() )
                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), objectType, SecurableObjectType.class )
                .set( CommonColumns.PERMISSIONS.cql(),
                        permission,
                        Permission.class );
    }

    private BoundStatement bindAuthorizedAclKeysQuery( Principal principal, Permission permission ) {
        return authorizedAclKeysQuery.bind()
                .set( CommonColumns.PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                .setString( CommonColumns.PRINCIPAL_ID.cql(), principal.getId() )
                .set( CommonColumns.PERMISSIONS.cql(), permission, Permission.class );
    }

    public Stream<Principal> getPrincipalsForSecurableObject( List<UUID> aclKeys ) {
        return Stream.of( session.executeAsync(
                aclsForSecurableObjectQuery.bind().setList( CommonColumns.ACL_KEYS.cql(),
                        aclKeys,
                        UUID.class ) ) )
                .map( ResultSetFuture::getUninterruptibly )
                .flatMap( StreamUtil::stream )
                .map( AuthorizationUtils::getPrincipalFromRow );
    }

    public Acl getAclsForSecurableObject( List<UUID> aclKeys ) {
        Stream<Ace> accessControlEntries = getPrincipalsForSecurableObject( aclKeys )
                .map( principal -> new AceKey( aclKeys, principal ) )
                .map( aceKey -> new AceFuture( aceKey.getPrincipal(), aces.getAsync( aceKey ) ) )
                .map( AceFuture::getUninterruptibly );
        return new Acl( aclKeys, accessControlEntries::iterator );

    }

    public void createEmptyAcl( List<UUID> aclKey, SecurableObjectType objectType ) {
        BoundStatement bs = setObjectType.bind()
                .setList( CommonColumns.ACL_KEYS.cql(), aclKey, UUID.class )
                .set( CommonColumns.SECURABLE_OBJECT_TYPE.cql(), objectType, SecurableObjectType.class );
        session.execute( bs );
        logger.info( "Created empty acl with key {} and type {}", aclKey, objectType );
    }

    public void deletePermissionsByAclKeys( List<UUID> aclKey ) {
        BoundStatement bs = deletePermissionsByAclKeysQuery.bind().setList( CommonColumns.ACL_KEYS.cql(),
                aclKey,
                UUID.class );
        session.execute( bs );
        logger.info( "Deleted all permissions for aclKey " + aclKey );
    }
    
    public Iterable<Principal> getOwnersForSecurableObject( List<UUID> aclKeys ) {
        BoundStatement bs = ownersForSecurableObjectQuery.bind().setList( CommonColumns.ACL_KEYS.cql(),
                aclKeys,
                UUID.class );
        return Iterables.transform( session.execute( bs ), AuthorizationUtils::getPrincipalFromRow ) ;
    }

}
