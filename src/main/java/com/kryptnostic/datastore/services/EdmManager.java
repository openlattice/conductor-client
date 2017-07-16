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
import java.util.stream.Stream;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.authorization.Principal;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.type.AssociationDetails;
import com.dataloom.edm.type.ComplexType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.EnumType;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.PropertyType;
import com.hazelcast.map.EntryProcessor;

public interface EdmManager {
    PropertyType getPropertyType( FullQualifiedName propertyTypeFqn );

    PropertyType getPropertyType( UUID propertyTypeId );

    void createPropertyTypeIfNotExists( PropertyType propertyType );

    void deletePropertyType( UUID propertyTypeId );

    Iterable<PropertyType> getPropertyTypesInNamespace( String namespace );

    Iterable<PropertyType> getPropertyTypes();

    void createEntitySet( Principal principal, EntitySet entitySet );

    //Warning: This method is used only in creating linked entity set, where entity set owner may not own all the property types.
    void createEntitySet( Principal principal, EntitySet entitySet, Set<UUID> ownablePropertyTypes );

    EntitySet getEntitySet( UUID entitySetId );

    Iterable<EntitySet> getEntitySets();

    void deleteEntitySet( UUID entitySetId );

    void createEntityType( EntityType objectType );

    EntityType getEntityType( String namespace, String name );

    Iterable<EntityType> getEntityTypes();
    
    Iterable<EntityType> getAssociationEntityTypes();
    
    void deleteEntityType( UUID entityTypeId );

    EntityType getEntityType( UUID entityTypeId );
    
    void addPropertyTypesToEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );

    void removePropertyTypesFromEntityType( UUID entityTypeId, Set<UUID> propertyTypeIds );
    
    void addSrcEntityTypesToAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds );
    
    void addDstEntityTypesToAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds );
    
    void removeSrcEntityTypesFromAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds );
    
    void removeDstEntityTypesFromAssociationType( UUID associationTypeId, Set<UUID> entityTypeIds );

    void updatePropertyTypeMetadata( UUID typeId, MetadataUpdate update );

    void updateEntityTypeMetadata( UUID typeId, MetadataUpdate update );

    void updateEntitySetMetadata( UUID typeId, MetadataUpdate update );

    // Helper methods to check existence
    boolean checkPropertyTypesExist( Set<UUID> properties );

    boolean checkPropertyTypeExists( UUID propertyTypeId );
    
    boolean checkEntityTypesExist( Set<UUID> entityTypeIds );

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
    
    EntityType getEntityTypeByEntitySetId( UUID entitySetId );

    void createEnumTypeIfNotExists( EnumType enumType );

    Stream<EnumType> getEnumTypes();

    EnumType getEnumType( UUID enumTypeId );

    void deleteEnumType( UUID enumTypeId );

    void createComplexTypeIfNotExists( ComplexType complexType );

    Stream<ComplexType> getComplexTypes();

    ComplexType getComplexType( UUID complexTypeId );

    void deleteComplexType( UUID complexTypeId );

    Set<EntityType> getEntityTypeHierarchy( UUID entityTypeId );

    Set<ComplexType> getComplexTypeHierarchy( UUID complexTypeId );
    
    UUID createAssociationType( AssociationType associationType, UUID entityTypeId );
    
    AssociationType getAssociationType( UUID associationTypeId );
            
    void deleteAssociationType( UUID associationTypeId );

    AssociationDetails getAssociationDetails( UUID associationTypeId );
    
    Iterable<EntityType> getAvailableAssociationTypesForEntityType( UUID entityTypeId );

}