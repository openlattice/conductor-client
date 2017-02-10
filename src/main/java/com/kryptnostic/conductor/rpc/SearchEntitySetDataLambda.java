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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class SearchEntitySetDataLambda implements Function<ConductorSparkApi, List<Map<String, Object>>>, Serializable {
    private static final long serialVersionUID = -3273005291047567056L;

    private UUID entitySetId;
    private String searchTerm;
    private Set<UUID> authorizedProperties;
    
    public SearchEntitySetDataLambda( UUID entitySetId, String searchTerm, Set<UUID> authorizedProperties) {
        this.entitySetId = entitySetId;
        this.searchTerm = searchTerm;
        this.authorizedProperties = authorizedProperties;
    }
    
    @Override
    public List<Map<String, Object>> apply( ConductorSparkApi api ) {
        return api.executeEntitySetDataSearch( entitySetId, searchTerm, authorizedProperties );
    }

}
