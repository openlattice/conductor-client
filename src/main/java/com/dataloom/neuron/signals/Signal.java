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

package com.dataloom.neuron.signals;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Principal;
import com.dataloom.client.serialization.SerializationConstants;
import com.dataloom.neuron.SignalType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class Signal {

    private static final Logger logger = LoggerFactory.getLogger( Signal.class );

    private SignalType       type;
    private List<UUID>       aclKey;
    private Principal        principal;
    private Optional<String> details;

    @JsonCreator
    public Signal(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) SignalType type,
            @JsonProperty( SerializationConstants.ACL_KEY ) List<UUID> aclKey,
            @JsonProperty( SerializationConstants.PRINCIPAL ) Principal principal,
            @JsonProperty( SerializationConstants.DETAILS_FIELD ) Optional<String> details ) {

        this.type = checkNotNull( type );
        this.details = checkNotNull( details );
        this.aclKey = checkNotNull( aclKey );
        this.principal = checkNotNull( principal );
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public SignalType getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.ACL_KEY )
    public List<UUID> getAclKey() {
        return aclKey;
    }

    @JsonProperty( SerializationConstants.PRINCIPAL )
    public Principal getPrincipal() {
        return principal;
    }

    @JsonProperty( SerializationConstants.DETAILS_FIELD )
    public Optional<String> getDetails() {
        return details;
    }

}
