package com.kryptnostic.datastore.services;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.auth0.spring.security.api.Auth0UserDetails;
import com.kryptnostic.datastore.Permission;

public class ActionAuthorizationService {

    private String username;
    private List<String>             currentRoles;

    private final PermissionsService ps;

    public ActionAuthorizationService( PermissionsService ps ) {
        this.ps = ps;
    };

    private void updateUserInfo() {
        Auth0UserDetails user = (Auth0UserDetails) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        
        //TODO: feel very worried by using username as identifier, since it will return email if exists, otherwise user_id. This means that username can potentially add emails to same identity and username will return differently. Attempts to retrieve userId so far has failed (always null).
        this.username = user.getUsername();
        this.currentRoles = user.getAuthorities().stream()
                .map( grantedAuthority -> grantedAuthority.getAuthority() ).collect( Collectors.toList() );
        /**
        currentRoles = SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream()
                .map( grantedAuthority -> grantedAuthority.getAuthority() ).collect( Collectors.toList() );
        */
    }

    /**
     * Entity Data Model actions
     */

    public boolean upsertPropertyType( FullQualifiedName propertyTypeFqn ) {
        updateUserInfo();
        return true;
    }

    public boolean upsertEntityType( FullQualifiedName entityTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        }else{
            return ps.checkUserHasPermissionsOnEntityType( username, currentRoles, entityTypeFqn, Permission.ALTER );
        }
    }

    public boolean upsertEntitySet( String entitySetName ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( username, currentRoles, entitySetName, Permission.ALTER );
        }
    }

    public boolean deletePropertyType( FullQualifiedName propertyTypeFqn ) {
        updateUserInfo();
        return true;
    }

    public boolean deleteEntityType( FullQualifiedName entityTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntityType( username, currentRoles, entityTypeFqn, Permission.ALTER );
        }
    }

    public boolean deleteEntitySet( String entitySetName ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( username, currentRoles, entitySetName, Permission.ALTER );
        }
    }

    public boolean getEntitySet( String entitySetName ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( username, currentRoles, entitySetName, Permission.DISCOVER );
        }
    }

    public boolean alterEntityType( FullQualifiedName entityTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntityType( username, currentRoles, entityTypeFqn, Permission.ALTER );
        }
    }

    public boolean alterEntitySet( String entitySetName ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( username, currentRoles, entitySetName, Permission.ALTER );
        }
    }

    public boolean assignEntityToEntitySet( String entitySetName ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( username, currentRoles, entitySetName, Permission.WRITE );
        }
    }

    public boolean readPropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( username, currentRoles,
                entityTypeFqn,
                propertyTypeFqn,
                Permission.READ );
        }
    }

    public boolean readPropertyTypeInEntitySet(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( username, currentRoles,
                entitySetName,
                propertyTypeFqn,
                Permission.READ );
        }
    }

    public boolean writePropertyTypeInEntityType( FullQualifiedName entityTypeFqn, FullQualifiedName propertyTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnPropertyTypeInEntityType( username, currentRoles,
                entityTypeFqn,
                propertyTypeFqn,
                Permission.WRITE );
        }
    }

    public boolean writePropertyTypeInEntitySet(
            String entitySetName,
            FullQualifiedName propertyTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( username, currentRoles,
                entitySetName,
                propertyTypeFqn,
                Permission.WRITE );
        }
    }

    /**
     * Data Service actions
     */

    public boolean getAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntityType( username, currentRoles, entityTypeFqn, Permission.READ );
        }
    }

    public boolean getAllEntitiesOfEntitySet( String entitySetName ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( username, currentRoles, entitySetName, Permission.READ );
        }
    }

    public boolean createEntityOfEntityType( FullQualifiedName entityTypeFqn ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntityType( username, currentRoles, entityTypeFqn, Permission.WRITE );
        }
    }

    public boolean createEntityOfEntitySet( String entitySetName ) {
        updateUserInfo();
        if( currentRoles.contains( Constants.ROLE_ADMIN ) ){
            return true;
        } else {
            return ps.checkUserHasPermissionsOnEntitySet( username, currentRoles, entitySetName, Permission.WRITE );
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
