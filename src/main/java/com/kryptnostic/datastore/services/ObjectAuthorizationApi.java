package com.kryptnostic.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public interface ObjectAuthorizationApi {
	
	void setReadAccess( UUID userId, Set<FullQualifiedName> types, Set<String> permissions );
	
	void removePermission( Set<FullQualifiedName> types, Set<String> permissions );

	void removePermission( FullQualifiedName type, Set<String> permissions );
	
	void setPermission( Set<FullQualifiedName> types, Set<String> permissions );

	void setPermission( FullQualifiedName type, Set<String> permissions );
	
	void checkPermission( FullQualifiedName type, String action );
}