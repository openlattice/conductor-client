package com.openlattice.data.storage

import java.net.URL

interface ByteBlobDataManager {
    fun putObject(s3Key: String, data: ByteArray)

    fun deleteObject(s3Key: String)

    fun getObjects(objects: List<Any>): List<Any>

    fun getBase64EncodedString(url: String): String
    fun getBase64EncodedStrings(urls: Set<String>): Map<String, String>
}