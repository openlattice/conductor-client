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

package com.dataloom.hazelcast;

/**
 * Simplifies management of type ids within Hazelcast for serialization. Can be re-ordered safely unless doing hot
 * upgrade. <b>NOTE: Leave first entry in place</b>
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public enum StreamSerializerTypeIds {
    /**
     * Move this one, break everything.
     */
    STREAM_SERIALIZER_IDS_MUST_BE_POSTIVE,
    ACL_KEY,
    AUDIT_METRIC,
    AUDIT_METRIC_INCREMENTER,
    RUNNABLE,
    CALLABLE,
    CONDUCTOR_CALL,
    EMPLOYEE,
    ENTITY_KEY,
    ENTITY_SET,
    ENTITY_TYPE,
    COMPLEX_TYPE,
    ENUM_TYPE,
    LINKING_EDGE,
    LINKING_VERTEX,
    LINKING_ENTITY_KEY,
    LINKING_VERTEX_KEY,
    PROPERTY_TYPE,
    PERMISSION_MERGER,
    PERMISSION_REMOVER,
    PRINCIPAL_MERGER,
    PRINCIPAL_REMOVER,
    FULL_QUALIFIED_NAME,
    QUERY_RESULT,
    EDM_PRIMITIVE_TYPE_KIND,
    UUID,
    ACE_KEY,
    PRINCIPAL,
    STRING_SET,
    PERMISSION_SET,
    ACLROOT_PRINCIPAL_PAIR,
    PERMISSIONS_REQUEST_DETAILS,
    PRINCIPAL_REQUESTID_PAIR,
    ACLROOT_REQUEST_DETAILS_PAIR,
    UUID_SET,
    PRINCIPAL_SET,
    TICKET_KEY,
    REQUEST_STATUS,
    REQUEST,
    STATUS,
    EDM_PRIMITIVE_TYPE_KIND_GETTER,
    ADD_SCHEMAS_TO_TYPE,
    REMOVE_SCHEMAS_FROM_TYPE,
    SCHEMA_MERGER,
    SCHEMA_REMOVER,
    ADD_PROPERTY_TYPES_TO_ENTITY_TYPE_PROCESSOR,
    REMOVE_PROPERTY_TYPES_FROM_ENTITY_TYPE_PROCESSOR,
    EMAIL_DOMAINS_MERGER,
    EMAIL_DOMAINS_REMOVER,
    UPDATE_REQUEST_STATUS_PROCESSOR,
    UPDATE_PROPERTY_TYPE_METADATA_PROCESSOR,
    UPDATE_ENTITY_TYPE_METADATA_PROCESSOR,
    UPDATE_ENTITY_SET_METADATA_PROCESSOR,
    ENTITY_KEY_SET,
    ENTITY,
    SEARCH_RESULT,
    CONDUCTOR_ELASTICSEARCH_CALL,
    ASSOCIATION_TYPE,
    ROLE_KEY,
    ORGANIZATION_ROLE,
    ROLE_TITLE_UPDATER,
    ROLE_DESCRIPTION_UPDATER,
    EDGE_KEY,
    LOOM_VERTEX,
    LOOM_EDGE,
}
