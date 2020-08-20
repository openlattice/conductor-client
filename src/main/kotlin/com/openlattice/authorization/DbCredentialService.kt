package com.openlattice.authorization

import com.google.common.base.MoreObjects
import com.hazelcast.core.HazelcastInstance
import com.openlattice.datastore.util.Util
import com.openlattice.hazelcast.HazelcastMap
import org.slf4j.LoggerFactory
import java.security.SecureRandom

class DbCredentialService(
        hazelcastInstance: HazelcastInstance
) {

    private val dbcreds = HazelcastMap.DB_CREDS.getMap(hazelcastInstance)

    companion object {
        private val logger = LoggerFactory.getLogger(DbCredentialService::class.java)

        private val upper = ('A'..'Z').toString()
        private val lower =  ('a'..'z').toString()
        private val digits = ('0'..'9').toString()
        private const val special = "!@#$%^&*()"
        private val srcBuf = (upper + lower + digits + special).toCharArray()

        private const val CREDENTIAL_LENGTH = 20
        private val r = SecureRandom()
    }

    fun getDbCredential(userId: String): String {
        return Util.getSafely(dbcreds, userId)
    }

    fun getOrCreateUserCredentials(userId: String?): String {
        logger.info("Generating credentials for user id {}", userId)
        val cred: String = generateCredential()
        logger.info("Generated credentials for user id {}", userId)
        return MoreObjects.firstNonNull(dbcreds.putIfAbsent(userId!!, cred), cred)
    }

    fun deleteUserCredential(userId: String?) {
        dbcreds.delete(userId!!)
    }

    fun rollUserCredential(userId: String?): String {
        val cred = generateCredential()
        dbcreds[userId] = cred
        return cred
    }

    private fun generateCredential(): String {
        val credential = CharArray(CREDENTIAL_LENGTH)
        for( i in 0 until CREDENTIAL_LENGTH) {
            credential[i] = srcBuf[ r.nextInt( srcBuf.size )]
        }
        return String(credential)
    }
}

