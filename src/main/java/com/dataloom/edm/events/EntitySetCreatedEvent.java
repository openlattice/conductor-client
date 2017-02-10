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

package com.dataloom.edm.events;

import java.util.List;

import com.dataloom.authorization.Principal;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;

public class EntitySetCreatedEvent {
	
	private EntitySet entitySet;
	private List<PropertyType> propertyTypes;
	private Principal principal;
	
	public EntitySetCreatedEvent( EntitySet entitySet, List<PropertyType> propertyTypes, Principal principal ) {
		this.entitySet = entitySet;
		this.propertyTypes = propertyTypes;
		this.principal = principal;
	}
	
	public EntitySet getEntitySet() {
		return entitySet;
	}
	
	public List<PropertyType> getPropertyTypes() {
		return propertyTypes;
	}
	
	public Principal getPrincipal() {
		return principal;
	}

}
