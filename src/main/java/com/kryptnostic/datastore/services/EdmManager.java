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

package com.kryptnostic.datastore.services;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.Principal;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.EntityType;
import com.dataloom.edm.PropertyType;
import com.hazelcast.map.EntryProcessor;

public interface EdmManager {
    PropertyType getPropertyType( FullQualifiedName propertyTypeFqn );

    PropertyType getPropertyType( UUID propertyTypeId );

    void createPropertyTypeIfNotExists( PropertyType propertyType );

    void deletePropertyType( UUID propertyTypeId );

    Iterable<PropertyType> getPropertyTypesInNamespace( String namespace );

    Iterable<PropertyType> getPropertyTypes();

    void createEntitySet( Principal principal, EntitySet entitySet );

    EntitySet getEntitySet( UUID entitySetId );

    Iterable<EntitySet> getEntitySets();

    void deleteEntitySet( UUID entitySetId );

    void createEntityType( EntityType objectType );

    void assignEntityToEntitySet( UUID syncId, String entityId, String name );

    void assignEntityToEntitySet( UUID syncId, String entityId, EntitySet es );

    EntityType getEntityType( String namespace, String name );

    Iterable<EntityType> getEntityTypes();

    void deleteEntityType( UUID entityTypeId );

    EntityType getEntityType( UUID entityTypeId );

    void addPropertyTypesToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );

    void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );
    
    void renamePropertyType( UUID typeId, FullQualifiedName newFqn );

    void renameEntityType( UUID typeId, FullQualifiedName newFqn );

    void renameEntitySet( UUID typeId, String newName );

    // Helper methods to check existence
    boolean checkPropertyTypesExist( Set<UUID> properties );

    boolean checkPropertyTypeExists( UUID propertyTypeId );

    boolean checkEntityTypeExists( UUID entityTypeId );

    boolean checkEntitySetExists( String name );

    Collection<PropertyType> getPropertyTypes( Set<UUID> properties );

    Set<UUID> getAclKeys( Set<?> fqnsOrNames );

    UUID getTypeAclKey( FullQualifiedName fqns );

    Set<UUID> getEntityTypeUuids( Set<FullQualifiedName> fqns );

    Set<UUID> getPropertyTypeUuids( Set<FullQualifiedName> fqns );

    EntityType getEntityType( FullQualifiedName type );

    EntitySet getEntitySet( String entitySetName );

    FullQualifiedName getPropertyTypeFqn( UUID propertyTypeId );

    FullQualifiedName getEntityTypeFqn( UUID entityTypeId );
    
    Map<UUID, PropertyType> getPropertyTypesAsMap( Set<UUID> propertyTypeIds );

    Map<UUID, EntityType> getEntityTypesAsMap( Set<UUID> entityTypeIds );

    Map<UUID, EntitySet> getEntitySetsAsMap( Set<UUID> entitySetIds );

    <V> Map<UUID, V> fromPropertyTypes( Set<UUID> propertyTypeIds, EntryProcessor<UUID, PropertyType> ep );

    Set<UUID> getPropertyTypeUuidsOfEntityTypeWithPIIField( UUID entityTypeId );

}