package com.openlattice.data.ids

import com.openlattice.TestServer
import org.junit.Assert
import org.junit.Test
import java.util.*

class IdCipherManagerTest: TestServer() {

    @Test(expected = IllegalArgumentException::class)
    fun testGenerateSecretKey() {
        val cipher = IdCipherManager(hazelcastInstance)
        val linkingEntitySetId = UUID.randomUUID()

        cipher.assignSecretKey(linkingEntitySetId)
        cipher.assignSecretKey(linkingEntitySetId)
    }

    @Test
    fun testEncryption() {
        val cipher = IdCipherManager(hazelcastInstance)
        val linkingEntitySetId1 = UUID.randomUUID()
        cipher.assignSecretKey(linkingEntitySetId1)
        val linkingEntitySetId2 = UUID.randomUUID()
        cipher.assignSecretKey(linkingEntitySetId2)

        val id = UUID.randomUUID()

        val eId11 = cipher.encryptId(linkingEntitySetId1, id)
        val eId12 = cipher.encryptId(linkingEntitySetId1, id)

        val eId21 = cipher.encryptId(linkingEntitySetId2, id)

        Assert.assertNotEquals(eId11, id)
        Assert.assertEquals(eId11, eId12)
        Assert.assertNotEquals(eId11, eId21)
    }

    @Test
    fun testDecryption() {
        val cipher = IdCipherManager(hazelcastInstance)
        val linkingEntitySetId = UUID.randomUUID()
        cipher.assignSecretKey(linkingEntitySetId)

        val id1 = UUID.randomUUID()

        val eId1 = cipher.encryptId(linkingEntitySetId, id1)
        val eId2 = cipher.encryptId(linkingEntitySetId, id1)

        val deId1 = cipher.decryptId(linkingEntitySetId, eId1)
        val deId2 = cipher.decryptId(linkingEntitySetId, eId2)

        Assert.assertEquals(deId1, deId2)
        Assert.assertEquals(deId1, id1)
    }
}