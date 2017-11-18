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

import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import com.zaxxer.hikari.HikariDataSource;
import java.security.SecureRandom;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class DbCredentialService {
    private static final String upper   = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String lower   = upper.toLowerCase();
    private static final String digits  = "0123456789";
    private static final String special = "!@#$%^&*()";
    private static final String source  = upper + lower + digits + special;
    private static final char[] srcBuf  = source.toCharArray();

    private static final int CREDENTIAL_LENGTH = 20;

    private static final SecureRandom r = new SecureRandom();

    private final IMap<String, String>     dbcreds;
    private final DbCredentialQueryService dcqs;

    public DbCredentialService( HazelcastInstance hazelcastInstance, HikariDataSource hds ) {
        this.dbcreds = hazelcastInstance.getMap( HazelcastMap.DB_CREDS.name() );
        this.dcqs = new DbCredentialQueryService( hds );
    }

    public String getDbCredential( String userId ) {
        return Util.getSafely( dbcreds, userId );
    }

    public String createUser( String userId ) {
        String cred = generateCredential();
        dcqs.createUser( userId, cred );
        return cred;
    }

    public String setDbCredential( String userId ) {
        String cred = generateCredential();
        dbcreds.set( userId, cred );
        return cred;
    }

    private String generateCredential() {
        char[] cred = new char[ CREDENTIAL_LENGTH ];
        for ( int i = 0; i < CREDENTIAL_LENGTH; ++i ) {
            cred[ i ] = srcBuf[ r.nextInt( srcBuf.length ) ];
        }
        return new String( cred );
    }
}
