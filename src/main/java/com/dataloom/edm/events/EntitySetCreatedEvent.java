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
