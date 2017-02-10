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

package com.dataloom.authorization.events;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.dataloom.authorization.Principal;

public class AclUpdateEvent {
    
    private List<UUID> aclKeys;
    private Set<Principal> principals;
    
    public AclUpdateEvent( List<UUID> aclKeys, Set<Principal> principals ) {
        this.aclKeys = aclKeys;
        this.principals = principals;
    }
    
    public List<UUID> getAclKeys() {
        return aclKeys;
    }
    
    public Set<Principal> getPrincipals() {
        return principals;
    }

}
