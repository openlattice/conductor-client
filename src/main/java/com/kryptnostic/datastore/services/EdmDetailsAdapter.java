package com.kryptnostic.datastore.services;

import com.google.common.base.Preconditions;
import com.kryptnostic.conductor.rpc.odata.EntitySet;

/**
 * Static factory that adds details to Edm objects
 * 
 * @author Ho Chung
 *
 */
public class EdmDetailsAdapter {

    private void EdmDetailsAdapterFactory() {}

    public static EntitySet setEntitySetTypename( CassandraTableManager ctb, EntitySet es ) {
        Preconditions.checkNotNull( es.getTypename(), "Entity set has no associated entity type." );
        return es.setType( ctb.getEntityTypeForTypename( es.getTypename() ) );
    }
}
