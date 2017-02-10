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

package com.dataloom.requests.processors;

import java.util.Map.Entry;

import com.dataloom.requests.PermissionsRequestDetails;
import com.dataloom.requests.RequestStatus;
import com.dataloom.requests.mapstores.AclRootPrincipalPair;
import com.google.common.base.Preconditions;
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor;

public class UpdateRequestStatusProcessor
        extends AbstractRhizomeEntryProcessor<AclRootPrincipalPair, PermissionsRequestDetails, Void> {

    private static final long serialVersionUID = 2664646492959861489L;
    private RequestStatus     status;

    public UpdateRequestStatusProcessor( RequestStatus status ) {
        this.status = status;
    }

    @Override
    public Void process( Entry<AclRootPrincipalPair, PermissionsRequestDetails> entry ) {
        PermissionsRequestDetails details = entry.getValue();
        Preconditions.checkNotNull( details, "Permissions Request does not exist." );
        details.setStatus( status );
        return null;
    }

    public RequestStatus getRequestStatus(){
        return status;
    }
}
