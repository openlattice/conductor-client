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

package com.kryptnostic.conductor.rpc;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.type.PropertyType;

public class Lambdas implements Serializable {
    private static final long serialVersionUID = -8384320983731367620L;

    public static Runnable foo() {
        return (Runnable & Serializable) () -> System.out.println( "UNSTOPPABLE" );
    }

    public static Function<ConductorSparkApi, QueryResult> getAllEntitiesOfType(
            FullQualifiedName fqn,
            List<PropertyType> authorizedProperties ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api.getAllEntitiesOfType( fqn,
                authorizedProperties );
    }

    public static Function<ConductorSparkApi, QueryResult> getFilteredEntities(
            LookupEntitiesRequest lookupEntitiesRequest ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api
                .getFilterEntities( lookupEntitiesRequest );
    }

    public static Function<ConductorSparkApi, QueryResult> getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName,
            List<PropertyType> authorizedProperties ) {
        return (Function<ConductorSparkApi, QueryResult> & Serializable) ( api ) -> api
                .getAllEntitiesOfEntitySet( entityFqn, entitySetName, authorizedProperties );
    }

    public static Function<ConductorSparkApi, Void> clustering( UUID linkedEntitySetId ) {
        return (Function<ConductorSparkApi, Void> & Serializable) ( api ) -> api
                .clustering( linkedEntitySetId );
    }
}