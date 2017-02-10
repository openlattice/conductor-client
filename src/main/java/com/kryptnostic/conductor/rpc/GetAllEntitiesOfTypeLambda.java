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
import java.util.function.Function;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.edm.internal.PropertyType;

public class GetAllEntitiesOfTypeLambda implements Function<ConductorSparkApi, QueryResult>, Serializable {

    private static final long serialVersionUID = 1L;

    private FullQualifiedName fqn;
    private List<PropertyType> properties;

    public GetAllEntitiesOfTypeLambda( FullQualifiedName fqn, List<PropertyType> authorizedProperties ) {
        this.fqn = fqn;
        this.properties = authorizedProperties;
    }

    @Override
    public QueryResult apply( ConductorSparkApi api ) {
        return api.getAllEntitiesOfType( fqn, properties );
    }
}