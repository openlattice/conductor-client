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

package com.kryptnostic.datastore.cassandra;

import org.junit.Test;

import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

public class CtbTest {

    //TODO: Move the test for this into Rhizome as the Table Builder lives there now.
    @Test
    public void testTableQuery() {
        System.out.println( new CassandraTableBuilder( Table.ENTITY_SETS.getName() ).ifNotExists()
                .partitionKey( CommonColumns.TYPE ,CommonColumns.ACLID )
                .clusteringColumns( CommonColumns.NAME )
                .columns( CommonColumns.TITLE ).buildCreateTableQuery() );
    }
}
