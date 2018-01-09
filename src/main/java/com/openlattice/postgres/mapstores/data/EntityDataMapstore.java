/*
 * Copyright (C) 2018. OpenLattice, Inc
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.postgres.mapstores.data;

import static com.openlattice.postgres.DataTables.LAST_INDEX;
import static com.openlattice.postgres.DataTables.LAST_WRITE;
import static com.openlattice.postgres.PostgresColumn.VERSION;

import com.dataloom.edm.type.PropertyType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.hazelcast.core.MapStore;
import com.openlattice.authorization.AclKey;
import com.openlattice.data.EntityDataMetadata;
import com.openlattice.data.EntityDataValue;
import com.openlattice.data.PropertyKey;
import com.openlattice.data.PropertyMetadata;
import com.openlattice.postgres.DataTables;
import com.openlattice.postgres.PostgresColumnDefinition;
import com.openlattice.postgres.PostgresTableDefinition;
import com.openlattice.postgres.ResultSetAdapters;
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class EntityDataMapstore extends AbstractBasePostgresMapstore<UUID, EntityDataValue> {
    private final Map<AclKey, PropertyDataMapstore> propertyMapstores;
    private final MapStore<UUID, PropertyType>      propertyTypes;
    private final UUID                              entitySetId;

    public EntityDataMapstore(
            HikariDataSource hds,
            PostgresTableDefinition table,
            UUID entitySetId,
            MapStore<UUID, PropertyType> propertyTypes ) {
        super( "edms", table, hds );
        this.propertyTypes = propertyTypes;
        this.entitySetId = entitySetId;
        this.propertyMapstores = Maps.newConcurrentMap();
    }

    @Override protected List<PostgresColumnDefinition> initValueColumns() {
        return ImmutableList.of( VERSION, LAST_WRITE, LAST_INDEX );
    }

    @Override public UUID generateTestKey() {
        return null;
    }

    @Override public EntityDataValue generateTestValue() {
        return null;
    }

    @Override protected void bind( PreparedStatement ps, UUID key, EntityDataValue value )
            throws SQLException {
        bind( ps, key, 1 );

        ps.setLong( 2, value.getVersion() );
        ps.setObject( 3, value.getLastWrite() );
        ps.setObject( 4, value.getLastIndex() );

        ps.setLong( 5, value.getVersion() );
        ps.setObject( 6, value.getLastWrite() );
        ps.setObject( 7, value.getLastIndex() );
    }

    @Override
    protected void handleStoreSucceeded( UUID key, EntityDataValue value ) {

    }

    @Override
    protected void handleStoreAllSucceeded( Map<UUID, EntityDataValue> m ) {
        for ( Entry<UUID, EntityDataValue> edEntry : m.entrySet() ) {
            final UUID edk = edEntry.getKey();
            final EntityDataValue edv = edEntry.getValue();
            final Map<UUID, Map<PropertyKey, PropertyMetadata>> properties = edv.getProperties();
            for ( Entry<UUID, Map<PropertyKey, PropertyMetadata>> propertyEntry : properties.entrySet() ) {
                UUID propertyTypeId = propertyEntry.getKey();
                PropertyDataMapstore pdm = getPropertyDataMapstore( entitySetId, propertyTypeId );
                pdm.storeAll( propertyEntry.getValue() );
            }
        }
    }

    @Override
    protected int bind( PreparedStatement ps, UUID key, int offset ) throws SQLException {
        ps.setObject( offset++, key );
        return offset;
    }

    @Override
    protected EntityDataValue mapToValue( ResultSet rs ) throws SQLException {
        UUID entityKeyId = ResultSetAdapters.id( rs );
        EntityDataMetadata entityDataMetadata = ResultSetAdapters.entityDataMetadata( rs );
        
        return rs.getO;
    }

    @Override
    protected UUID mapToKey( ResultSet rs ) throws SQLException {
        return null;
    }

    public PropertyDataMapstore getPropertyDataMapstore( UUID entitySetId, UUID propertyTypeId ) {
        return propertyMapstores
                .computeIfAbsent( new AclKey( entitySetId, propertyTypeId ), this::newPropertyDataMapstore );
    }

    protected PropertyDataMapstore newPropertyDataMapstore( AclKey aclKey ) {
        UUID entitySetId = aclKey.get( 0 );
        UUID propertyTypeId = aclKey.get( 1 );
        PropertyType propertyType = propertyTypes.load( propertyTypeId );
        PostgresTableDefinition table = DataTables.buildPropertyTableDefinition( entitySetId, propertyType );
        return new PropertyDataMapstore( table, hds );
    }
}
