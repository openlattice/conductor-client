package com.dataloom.hazelcast;

/**
 * Simplifies management of type ids within Hazelcast for serialization. Can be re-ordered safely unless doing hot upgrade.
 * <b>NOTE: Leave first entry in place</b> 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt; 
 *
 */
public enum StreamSerializerTypeIds {
    /**
     * Move this one, break everything.
     */
    STREAM_SERIALIZER_IDS_MUST_BE_POSTIVE,
    RUNNABLE,
    CALLABLE,
    CONDUCTOR_CALL,
    EMPLOYEE,
    ENTITY_SET,
    ENTITY_TYPE,
    PROPERTY_TYPE,
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
    PRINCIPAL_SET
}
