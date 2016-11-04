package com.kryptnostic.datastore.services;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.security.core.context.SecurityContextHolder;

import com.kryptnostic.datastore.Permission;

public class ActionAuthorizationService {

    private List<String>             currentRoles;

    private final PermissionsService ps;

    public ActionAuthorizationService( PermissionsService ps ) {
        this.ps = ps;
    };

    private void updateRoles() {
        currentRoles = SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream()
                .map( grantedAuthority -> grantedAuthority.getAuthority() ).collect( Collectors.toList() );
    }

    /**
     * Entity Data Model actions
     */

    public boolean upsertPropertyType( FullQualifiedName propertyTypeFqn ) {
        updateRoles();
        return true;
    }

    public boolean upsertEntityType( FullQualifiedName entityTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        }else{
            return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.ALTER );
        }
    }

    public boolean upsertEntitySet( String entitySetName ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entitySetName, Permission.ALTER );
        }
    }

    public boolean deletePropertyType( FullQualifiedName propertyTypeFqn ) {
        updateRoles();
        return true;
    }

    public boolean deleteEntityType( FullQualifiedName entityTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.ALTER );
        }
    }

    public boolean deleteEntitySet( String entitySetName ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entitySetName, Permission.ALTER );
        }
    }

    public boolean getEntitySet( String entitySetName ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entitySetName, Permission.DISCOVER );
        }
    }

    public boolean alterEntityType( FullQualifiedName entityTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.ALTER );
        }
    }

    public boolean alterEntitySet( String entitySetName ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entitySetName, Permission.ALTER );
        }
    }

    public boolean assignEntityToEntitySet( String entitySetName ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entitySetName, Permission.WRITE );
        }
    }

    public boolean readPropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( currentRoles,
                entityTypeFqn,
                propertyTypeFqn,
                Permission.READ );
        }
    }

    public boolean readPropertyTypeInEntitySet(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( currentRoles,
                entitySetName,
                propertyTypeFqn,
                Permission.READ );
        }
    }

    public boolean writePropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( currentRoles,
                entityTypeFqn,
                propertyTypeFqn,
                Permission.WRITE );
        }
    }

    public boolean writePropertyTypeInEntitySet(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( currentRoles,
                entitySetName,
                propertyTypeFqn,
                Permission.WRITE );
        }
    }

    /**
     * Data Service actions
     */

    public boolean getAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.READ );
        }
    }

    public boolean getAllEntitiesOfEntitySet( String entitySetName ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entitySetName, Permission.READ );
        }
    }

    public boolean createEntityOfEntityType( FullQualifiedName entityTypeFqn ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntityType( currentRoles, entityTypeFqn, Permission.WRITE );
        }
    }

    public boolean createEntityOfEntitySet( String entitySetName ) {
        updateRoles();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( currentRoles, entitySetName, Permission.WRITE );
        }
    }

    /**
     * Permissions modification actions
     */
    public boolean updateEntityTypesAcls() {
        return true;
    }

    public boolean updateEntitySetsAcls() {
        return true;
    }

    public boolean updatePropertyTypeInEntityTypeAcls() {
        return true;
    }

    public boolean updatePropertyTypeInEntitySetAcls() {
        return true;
    }

    public boolean removeEntityTypeAcls() {
        return true;
    }

    public boolean removeEntitySetAcls() {
        return true;
    }

    public boolean removePropertyTypeInEntityTypeAcls() {
        return true;
    }

    public boolean removeAllPropertyTypesInEntityTypeAcls() {
        return true;
    }

    public boolean removePropertyTypeInEntitySetAcls() {
        return true;
    }

    public boolean removeAllPropertyTypesInEntitySetAcls() {
        return true;
    }

}
