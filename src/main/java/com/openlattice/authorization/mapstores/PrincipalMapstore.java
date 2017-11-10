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

package com.openlattice.authorization.mapstores;

import com.dataloom.authorization.Principal;
import com.datastax.driver.core.Session;
import com.kryptnostic.rhizome.mapstores.TestableSelfRegisteringMapStore;
import com.openlattice.authorization.SecurablePrincipal;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PrincipalMapstore implements TestableSelfRegisteringMapStore<Principal, SecurablePrincipal> {
    private final HikariDataSource hds;
    private final String           mapName;
    private final Session          session;

    public PrincipalMapstore( HikariDataSource hds, String mapName, Session session ) {
        this.hds = hds;
        this.mapName = mapName;
        this.session = session;
    }


}
