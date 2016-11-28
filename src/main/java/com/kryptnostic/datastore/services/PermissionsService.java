package com.kryptnostic.datastore.services;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.PermissionsInfo;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.authorization.requests.PropertyTypeInEntitySetAclRequestWithRequestingUser;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.requests.PropertyTypeInEntitySetAclRequest;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.ResultSetAdapterFactory;
import com.kryptnostic.datastore.exceptions.BadRequestException;

public class PermissionsService implements PermissionsManager {

    private final Session               session;
    private final Mapper<EntityType>    entityTypeMapper;
    private final CassandraTableManager tableManager;
    private final CassandraEdmStore     edmStore;

    public PermissionsService( Session session, MappingManager mappingManager, CassandraTableManager tableManager ) {
        this.session = session;
        this.tableManager = tableManager;
        this.entityTypeMapper = mappingManager.mapper( EntityType.class );
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
    }

    @Override
    public void addPermissionsForEntityType( Principal principal, FullQualifiedName fqn, Set<Permission> permissions ) {
        switch( principal.getType() ){
            case ROLE:
                tableManager.addRolePermissionsForEntityType( principal.getName(), fqn, permissions );    
                break;
            case USER:
                tableManager.addUserPermissionsForEntityType( principal.getName(), fqn, permissions );          
                break;
            default:
                throw new BadRequestException("Principal has undefined type.");
        }
    }

    @Override
    public void removePermissionsForEntityType( Principal principal, FullQualifiedName fqn, Set<Permission> permissions ) {
        EnumSet<Permission> userPermissions = getPermissionsForEntityType( principal, fqn );
        userPermissions.removeAll( permissions );

        setPermissionsForEntityType( principal, fqn, userPermissions );
    }

    @Override
    public void setPermissionsForEntityType( Principal principal, FullQualifiedName fqn, Set<Permission> permissions ) {
        switch( principal.getType() ){
            case ROLE:
                if( !permissions.isEmpty() ){
                    tableManager.setRolePermissionsForEntityType( principal.getName(), fqn, permissions );
                } else {
                    tableManager.deleteRoleAndTypeFromEntityTypesAclsTable( principal.getName(), fqn );
                }
                break;
            case USER:
                if( !permissions.isEmpty() ){
                    tableManager.setUserPermissionsForEntityType( principal.getName(), fqn, permissions );
                } else {
                    tableManager.deleteUserAndTypeFromEntityTypesAclsTable( principal.getName(), fqn );
                }
                break;
            default:
                throw new BadRequestException("Principal has undefined type.");                
        }
    }

    @Override
    public boolean checkUserHasPermissionsOnEntityType(
            String userId,
            List<String> roles,
            FullQualifiedName fqn,
            Permission permission ) {
        EnumSet<Permission> userPermissions = getPermissionsForEntityType( userId, roles, fqn );

        return userPermissions.contains( permission );
    }

    private EnumSet<Permission> getRolePermissionsForEntityType( String role, FullQualifiedName fqn ) {
        return tableManager.getRolePermissionsForEntityType( role, fqn );
    }

    private EnumSet<Permission> getRolePermissionsForEntityType( List<String> roles, FullQualifiedName fqn ) {
        return roles.stream()
                .flatMap( role -> getRolePermissionsForEntityType( role, fqn ).stream() )
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Permission.class)));
    }

    private EnumSet<Permission> getUserPermissionsForEntityType( String userId, FullQualifiedName fqn ) {
        return tableManager.getUserPermissionsForEntityType( userId, fqn );
    }
    
    private EnumSet<Permission> getPermissionsForEntityType( Principal principal, FullQualifiedName fqn ) {
        switch( principal.getType() ){
            case ROLE:
                return tableManager.getRolePermissionsForEntityType( principal.getName(), fqn );
            case USER:
                return tableManager.getUserPermissionsForEntityType( principal.getName(), fqn );
            default:
                throw new BadRequestException("Principal has undefined type.");                   
        }
    }
    
    private EnumSet<Permission> getPermissionsForEntityType( String userId, List<String> roles, FullQualifiedName fqn ) {
        EnumSet<Permission> userPermissions = getRolePermissionsForEntityType( roles, fqn );
        userPermissions.addAll( getUserPermissionsForEntityType( userId, fqn) );

        return userPermissions;
    }


    @Override
    public void addPermissionsForEntitySet(
            Principal principal,
            String name,
            Set<Permission> permissions ) {
        switch( principal.getType() ){
            case ROLE:
                tableManager.addRolePermissionsForEntitySet( principal.getName(), name, permissions );
                break;
            case USER:
                tableManager.addUserPermissionsForEntitySet( principal.getName(), name, permissions );          
                break;
            default:
                throw new BadRequestException("Principal has undefined type.");
        }
    }

    @Override
    public void removePermissionsForEntitySet(
            Principal principal,
            String name,
            Set<Permission> permissions ) {
        EnumSet<Permission> userPermissions = getPermissionsForEntitySet( principal, name );
        userPermissions.removeAll( permissions );

        setPermissionsForEntitySet( principal, name, userPermissions );
    }

    @Override
    public void setPermissionsForEntitySet(
            Principal principal,
            String name,
            Set<Permission> permissions ) {
        switch( principal.getType() ){
            case ROLE:
                if( !permissions.isEmpty() ){
                    tableManager.setRolePermissionsForEntitySet( principal.getName(), name, permissions );
                } else {
                    tableManager.deleteRoleAndSetFromEntitySetsAclsTable( principal.getName(), name );
                }
                break;
            case USER:
                if( !permissions.isEmpty() ){
                    tableManager.setUserPermissionsForEntitySet( principal.getName(), name, permissions );
                } else {
                    tableManager.deleteUserAndSetFromEntitySetsAclsTable( principal.getName(), name );
                }
                break;
            default:
                throw new BadRequestException("Principal has undefined type.");                
        }
    }

    @Override
    public boolean checkUserHasPermissionsOnEntitySet(
            String userId,
            List<String> roles,
            String name,
            Permission permission ) {
        EnumSet<Permission> userPermissions = getPermissionsForEntitySet( userId, roles, name );

        return userPermissions.contains( permission );
    }
    
    @Override
    public boolean checkIfUserIsOwnerOfEntitySet(
            String userId,
            String entitySetName ) {
        return tableManager.checkIfUserIsOwnerOfEntitySet( userId, entitySetName );
    }
    
    @Override
    public boolean checkIfUserIsOwnerOfEntitySet(
            String userId,
            UUID requestId ) {
        String entitySetName = tableManager.getEntitySetNameFromRequestId( requestId );
        return checkIfUserIsOwnerOfEntitySet( userId, entitySetName );
    }
    
    @Override
    public boolean checkIfUserIsOwnerOfPermissionsRequest(
            String userId,
            UUID requestId ) {
        return tableManager.checkIfUserIsOwnerOfPermissionsRequest( userId, requestId );
    }

    private EnumSet<Permission> getRolePermissionsForEntitySet( String role, String name ) {
        return tableManager.getRolePermissionsForEntitySet( role, name );
    }
    
    private EnumSet<Permission> getRolePermissionsForEntitySet( List<String> roles, String name ) {
        return roles.stream()
                .flatMap( role -> getRolePermissionsForEntitySet( role, name ).stream() )
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Permission.class)));
    }
    
    private EnumSet<Permission> getUserPermissionsForEntitySet( String user, String name ) {
        return tableManager.getUserPermissionsForEntitySet( user, name );
    }

    private EnumSet<Permission> getPermissionsForEntitySet( Principal principal, String name ){
        switch( principal.getType() ){
            case ROLE:
                return tableManager.getRolePermissionsForEntitySet( principal.getName(), name );
            case USER:
                return tableManager.getUserPermissionsForEntitySet( principal.getName(), name );
            default:
                throw new BadRequestException("Principal has undefined type.");                   
        }        
    }

    private EnumSet<Permission> getPermissionsForEntitySet( String userId, List<String> roles, String name ){
        EnumSet<Permission> userPermissions = getRolePermissionsForEntitySet( roles, name );
        userPermissions.addAll( getUserPermissionsForEntitySet( userId, name ) );
        
        return userPermissions;      
    }

    @Override
    public void addPermissionsForPropertyTypeInEntityType(
            Principal principal,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        switch( principal.getType() ){
            case ROLE:
                tableManager.addRolePermissionsForPropertyTypeInEntityType( principal.getName(), entityTypeFqn, propertyTypeFqn, permissions );
                break;
            case USER:
                tableManager.addUserPermissionsForPropertyTypeInEntityType( principal.getName(), entityTypeFqn, propertyTypeFqn, permissions );
                break;
            default:
                throw new BadRequestException("Principal has undefined type.");
        }
    }

    @Override
    public void removePermissionsForPropertyTypeInEntityType(
            Principal principal,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        EnumSet<Permission> userPermissions = getPermissionsForPropertyTypeInEntityType( principal, entityTypeFqn, propertyTypeFqn );
        userPermissions.removeAll( permissions );
        
        setPermissionsForPropertyTypeInEntityType( principal, entityTypeFqn, propertyTypeFqn, userPermissions);
    }

    @Override
    public void setPermissionsForPropertyTypeInEntityType(
            Principal principal,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        switch( principal.getType() ){
            case ROLE:
                if( !permissions.isEmpty() ){
                    tableManager.setRolePermissionsForPropertyTypeInEntityType( principal.getName(), entityTypeFqn, propertyTypeFqn, permissions );
                } else {
                    tableManager.deleteRoleAndTypesFromPropertyTypesInEntityTypesAclsTable( principal.getName(), entityTypeFqn, propertyTypeFqn );
                }
                break;
            case USER:
                if( !permissions.isEmpty() ){
                    tableManager.setUserPermissionsForPropertyTypeInEntityType( principal.getName(), entityTypeFqn, propertyTypeFqn, permissions );
                } else {
                    tableManager.deleteUserAndTypesFromPropertyTypesInEntityTypesAclsTable( principal.getName(), entityTypeFqn, propertyTypeFqn );
                }
                break;
            default:
                throw new BadRequestException("Principal has undefined type.");                
        }
    }

    @Override
    public boolean checkUserHasPermissionsOnPropertyTypeInEntityType(
            String userId,
            List<String> roles,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn,
            Permission permission ) {
        EnumSet<Permission> userPermissions = getPermissionsForPropertyTypeInEntityType( userId, roles, entityTypeFqn, propertyTypeFqn );

        return userPermissions.contains( permission );
    }

    private EnumSet<Permission> getRolePermissionsForPropertyTypeInEntityType(
            String role,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        return tableManager.getRolePermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn );
    }

    private EnumSet<Permission> getRolePermissionsForPropertyTypeInEntityType(
            List<String> roles,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        return roles.stream()
                .flatMap( role -> getRolePermissionsForPropertyTypeInEntityType( role, entityTypeFqn, propertyTypeFqn ).stream() )
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Permission.class)));
    }

    private EnumSet<Permission> getUserPermissionsForPropertyTypeInEntityType(
            String user,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        return tableManager.getUserPermissionsForPropertyTypeInEntityType( user, entityTypeFqn, propertyTypeFqn );
    }
    
    private EnumSet<Permission> getPermissionsForPropertyTypeInEntityType(
            Principal principal,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        switch( principal.getType() ){
            case ROLE:
                return tableManager.getRolePermissionsForPropertyTypeInEntityType( principal.getName(), entityTypeFqn, propertyTypeFqn );
            case USER:
                return tableManager.getUserPermissionsForPropertyTypeInEntityType( principal.getName(), entityTypeFqn, propertyTypeFqn );
            default:
                throw new BadRequestException("Principal has undefined type.");                   
        }    
    }
    
    private EnumSet<Permission> getPermissionsForPropertyTypeInEntityType(
            String userId,
            List<String> roles,
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        EnumSet<Permission> userPermissions = getRolePermissionsForPropertyTypeInEntityType( roles, entityTypeFqn, propertyTypeFqn );
        userPermissions.addAll( getUserPermissionsForPropertyTypeInEntityType( userId, entityTypeFqn, propertyTypeFqn ) );
        
        return userPermissions;  
    }
    
    @Override
    public void addPermissionsForPropertyTypeInEntitySet(
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        switch( principal.getType() ){
            case ROLE:
                tableManager.addRolePermissionsForPropertyTypeInEntitySet( principal.getName(), entitySetName, propertyTypeFqn, permissions );
                break;
            case USER:
                tableManager.addUserPermissionsForPropertyTypeInEntitySet( principal.getName(), entitySetName, propertyTypeFqn, permissions );
                break;
            default:
                throw new BadRequestException("Principal has undefined type.");
        }
    }

    @Override
    public void removePermissionsForPropertyTypeInEntitySet(
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        EnumSet<Permission> userPermissions = getPermissionsForPropertyTypeInEntitySet( principal,
                entitySetName,
                propertyTypeFqn );
        userPermissions.removeAll( permissions );

        setPermissionsForPropertyTypeInEntitySet( principal,
                entitySetName,
                propertyTypeFqn,
                userPermissions );
    }

    @Override
    public void setPermissionsForPropertyTypeInEntitySet(
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Set<Permission> permissions ) {
        switch( principal.getType() ){
            case ROLE:
                if( !permissions.isEmpty() ){
                    tableManager.setRolePermissionsForPropertyTypeInEntitySet( principal.getName(), entitySetName, propertyTypeFqn, permissions );
                } else {
                    tableManager.deleteRoleAndSetFromPropertyTypesInEntitySetsAclsTable( principal.getName(), entitySetName, propertyTypeFqn );
                }
                break;
            case USER:
                if( !permissions.isEmpty() ){
                    tableManager.setUserPermissionsForPropertyTypeInEntitySet( principal.getName(), entitySetName, propertyTypeFqn, permissions );
                } else {
                    tableManager.deleteUserAndSetFromPropertyTypesInEntitySetsAclsTable( principal.getName(), entitySetName, propertyTypeFqn );
                }
                break;
            default:
                throw new BadRequestException("Principal has undefined type.");                
        }
    }

    @Override
    public boolean checkUserHasPermissionsOnPropertyTypeInEntitySet(
            String userId,
            List<String> roles,
            String entitySetName,
            FullQualifiedName propertyTypeFqn,
            Permission permission ) {
        EnumSet<Permission> userPermissions = getPermissionsForPropertyTypeInEntitySet( userId, 
                roles,
                entitySetName,
                propertyTypeFqn );
        
        return userPermissions.contains( permission );
    }

    private EnumSet<Permission> getRolePermissionsForPropertyTypeInEntitySet(
            String role,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        return tableManager.getRolePermissionsForPropertyTypeInEntitySet( role,
                entitySetName,
                propertyTypeFqn );
    }

    private EnumSet<Permission> getRolePermissionsForPropertyTypeInEntitySet(
            List<String> roles,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        return roles.stream()
                .flatMap( role -> getRolePermissionsForPropertyTypeInEntitySet( role,
                        entitySetName,
                        propertyTypeFqn ).stream() )
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Permission.class)));
    }
    
    private EnumSet<Permission> getUserPermissionsForPropertyTypeInEntitySet(
            String user,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        return tableManager.getUserPermissionsForPropertyTypeInEntitySet( user,
                entitySetName,
                propertyTypeFqn );
    }
    
    private EnumSet<Permission> getPermissionsForPropertyTypeInEntitySet(
            Principal principal,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        switch( principal.getType() ){
            case ROLE:
                return tableManager.getRolePermissionsForPropertyTypeInEntitySet( principal.getName(), entitySetName, propertyTypeFqn );
            case USER:
                return tableManager.getUserPermissionsForPropertyTypeInEntitySet( principal.getName(), entitySetName, propertyTypeFqn );
            default:
                throw new BadRequestException("Principal has undefined type.");                   
        }
    }
    
    private EnumSet<Permission> getPermissionsForPropertyTypeInEntitySet(
            String userId,
            List<String> roles,
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        EnumSet<Permission> userPermissions = getRolePermissionsForPropertyTypeInEntitySet( roles, entitySetName, propertyTypeFqn );
        userPermissions.addAll( getUserPermissionsForPropertyTypeInEntitySet( userId, entitySetName, propertyTypeFqn ) );
        
        return userPermissions;  
    }
    
    @Override
    public void removePermissionsForEntityType( FullQualifiedName fqn ) {
        tableManager.deleteEntityTypeFromEntityTypesAclsTable( fqn );
    }

    @Override
    public void removePermissionsForEntitySet( String entitySetName ) {
        tableManager.deleteEntitySetFromEntitySetsAclsTable( entitySetName );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntityType(
            FullQualifiedName entityTypeFqn,
            FullQualifiedName propertyTypeFqn ) {
        tableManager.deleteTypesFromPropertyTypesInEntityTypesAclsTable( entityTypeFqn, propertyTypeFqn );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntityType( FullQualifiedName entityTypeFqn ) {
        tableManager.deleteEntityTypeFromPropertyTypesInEntityTypesAclsTable( entityTypeFqn );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntitySet(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        tableManager.deleteSetAndTypeFromPropertyTypesInEntitySetsAclsTable( entitySetName, propertyTypeFqn );
    }

    @Override
    public void removePermissionsForPropertyTypeInEntitySet( String entitySetName ) {
        tableManager.deleteSetFromPropertyTypesInEntitySetsAclsTable( entitySetName );
    }

    @Override
    public EnumSet<Permission> getEntitySetAclsForUser( String userId, List<String> currentRoles, String entitySetName ){
        return getPermissionsForEntitySet( userId, currentRoles, entitySetName );
    }
    
    @Override
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsForUser( String userId, List<String> currentRoles, String entitySetName ){
        EntitySet es = EdmDetailsAdapter.setEntitySetTypename( tableManager, edmStore.getEntitySet( entitySetName ) );    
        EntityType entityType = entityTypeMapper.get( es.getType().getNamespace(), es.getType().getName() );
        
        Map<FullQualifiedName, EnumSet<Permission>> userPermissions = entityType.getProperties()
                .stream()
                .collect( Collectors.toMap( fqn -> fqn, fqn -> getPermissionsForPropertyTypeInEntitySet( userId, currentRoles, entitySetName, fqn) ) );
        
        return userPermissions;
    }

    @Override
    public EnumSet<Permission> getEntityTypeAclsForUser( String userId, List<String> currentRoles, FullQualifiedName entityTypeFqn ){
        return getPermissionsForEntityType( userId, currentRoles, entityTypeFqn );
    }
    
    @Override
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntityTypeAclsForUser( String userId, List<String> currentRoles, FullQualifiedName entityTypeFqn ){
        EntityType entityType = entityTypeMapper.get( entityTypeFqn.getNamespace(), entityTypeFqn.getName() );
        
        Map<FullQualifiedName, EnumSet<Permission>> userPermissions = entityType.getProperties()
                .stream()
                .collect( Collectors.toMap( fqn -> fqn, fqn -> getPermissionsForPropertyTypeInEntityType( userId, currentRoles, entityTypeFqn, fqn) ) );
        
        return userPermissions;
    }

    @Override
    public Iterable<PermissionsInfo> getEntitySetAclsForOwner( String entitySetName ){
        Iterable<PermissionsInfo> roleAcls = Iterables.transform( tableManager.getRoleAclsForEntitySet( entitySetName ), ResultSetAdapterFactory::mapRoleRowToPermissionsInfo );
        Iterable<PermissionsInfo> userAcls = Iterables.transform( tableManager.getUserAclsForEntitySet( entitySetName ), ResultSetAdapterFactory::mapUserRowToPermissionsInfo );
        
        return Iterables.concat( roleAcls, userAcls );
    }
    
    @Override
    public Iterable<PermissionsInfo> getPropertyTypesInEntitySetAclsForOwner( String entitySetName, FullQualifiedName propertyTypeFqn ){
        Iterable<PermissionsInfo> roleAcls = Iterables.transform( tableManager.getRoleAclsForPropertyTypeInEntitySetBySetAndType( entitySetName, propertyTypeFqn ), ResultSetAdapterFactory::mapRoleRowToPermissionsInfo );
        Iterable<PermissionsInfo> userAcls = Iterables.transform( tableManager.getUserAclsForPropertyTypeInEntitySetBySetAndType( entitySetName, propertyTypeFqn ), ResultSetAdapterFactory::mapUserRowToPermissionsInfo );
        
        return Iterables.concat( roleAcls, userAcls );
    }
    
    @Override
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsOfPrincipalForOwner( String entitySetName, Principal principal ){
        EntitySet es = EdmDetailsAdapter.setEntitySetTypename( tableManager, edmStore.getEntitySet( entitySetName ) );    
        EntityType entityType = entityTypeMapper.get( es.getType().getNamespace(), es.getType().getName() );
        
        Map<FullQualifiedName, EnumSet<Permission>> userPermissions = entityType.getProperties()
                .stream()
                .collect( Collectors.toMap( fqn -> fqn, fqn -> getPermissionsForPropertyTypeInEntitySet( principal, entitySetName, fqn) ) );
        
        return userPermissions;
    }

    @Override
    public void addPermissionsRequestForPropertyTypeInEntitySet( String userId, Principal principal, String entitySetName, FullQualifiedName propertyTypeFqn, EnumSet<Permission> permissions ){
        tableManager.addPermissionsRequestForPropertyTypeInEntitySet( userId, principal, entitySetName, propertyTypeFqn, permissions);
    }

    @Override
    public void removePermissionsRequestForEntitySet( UUID id ){
        tableManager.removePermissionsRequestForEntitySet( id );
    }
    
    @Override
    public Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllReceivedRequestsForPermissionsOfUserId( String userId ){
        return Iterables.concat( getAllReceivedRequestsForPermissionsOfUserId(PrincipalType.ROLE, userId), getAllReceivedRequestsForPermissionsOfUserId(PrincipalType.USER, userId) );
    }

    private Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllReceivedRequestsForPermissionsOfUserId( PrincipalType type, String userId ){
        return Iterables.transform( tableManager.getAllReceivedRequestsForPermissionsOfUserId( type, userId ), row -> ResultSetAdapterFactory.mapRowToPropertyTypeInEntitySetAclRequestWithRequestingUser( type, row) );        
    }

    @Override
    public Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllReceivedRequestsForPermissionsOfEntitySet( String entitySetName ){
        return Iterables.concat( getAllReceivedRequestsForPermissionsOfEntitySet(PrincipalType.ROLE, entitySetName), getAllReceivedRequestsForPermissionsOfEntitySet(PrincipalType.USER, entitySetName) );
    }
    
    private Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllReceivedRequestsForPermissionsOfEntitySet( PrincipalType type, String entitySetName ){
        return Iterables.transform( tableManager.getAllReceivedRequestsForPermissionsOfEntitySet( type, entitySetName ), row -> ResultSetAdapterFactory.mapRowToPropertyTypeInEntitySetAclRequestWithRequestingUser( type, row) );        
    }

    @Override
    public Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllSentRequestsForPermissions( String userId ){
        return Iterables.concat( getAllSentRequestsForPermissions(PrincipalType.ROLE, userId), getAllSentRequestsForPermissions(PrincipalType.USER, userId) );
    }
    
    private Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllSentRequestsForPermissions( PrincipalType type, String userId ){
        return Iterables.transform( tableManager.getAllSentRequestsForPermissions( type, userId ), row -> ResultSetAdapterFactory.mapRowToPropertyTypeInEntitySetAclRequestWithRequestingUser( type, row) );        
    }

    @Override
    public Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllSentRequestsForPermissions( String userId, String entitySetName ){
        return Iterables.concat( getAllSentRequestsForPermissions(PrincipalType.ROLE, userId, entitySetName), getAllSentRequestsForPermissions(PrincipalType.USER, userId, entitySetName) );
    }
    
    private Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllSentRequestsForPermissions( PrincipalType type, String userId, String entitySetName ){
        return Iterables.transform( tableManager.getAllSentRequestsForPermissions( type, userId, entitySetName ), row -> ResultSetAdapterFactory.mapRowToPropertyTypeInEntitySetAclRequestWithRequestingUser( type, row) );
    }
}
