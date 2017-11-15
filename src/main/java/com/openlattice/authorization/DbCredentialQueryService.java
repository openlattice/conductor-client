/*
 * Copyright (C) 2017. OpenLattice, Inc
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

package com.openlattice.authorization;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DbCredentialQueryService {
    private static final String SET_PASSWORD = "ALTER USER ? WITH PASSWORD ?";
    private static final String CREATE_USER  = "CREATE USER ? WITH PASSWORD ?";
    private final HikariDataSource hds;

    public DbCredentialQueryService( HikariDataSource hds ) {
        this.hds = hds;
    }

    public void createUser( String userId, String credential ) {
        try ( Connection conn = hds.getConnection(); PreparedStatement ps = conn.prepareStatement( SET_PASSWORD ) ) {

            ps.setString( 1, userId );
            ps.setString( 2, credential );
            ps.execute();
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }

    public void setCredential( String userId, String credential ) {

    }
}
