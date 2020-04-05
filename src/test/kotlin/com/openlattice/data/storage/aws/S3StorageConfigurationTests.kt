package com.openlattice.data.storage.aws

import com.openlattice.datastore.configuration.S3StorageConfiguration
import com.openlattice.serializer.AbstractJacksonSerializationTest
import com.openlattice.serializer.AbstractJacksonYamlSerializationTest
import org.junit.Test

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class S3StorageConfigurationTests : AbstractJacksonYamlSerializationTest<S3StorageConfiguration>() {
    override fun getSampleData(): S3StorageConfiguration {
        return S3StorageConfiguration(
                "foobucket",
                "fooregion",
                "fooaccesskeyid",
                "foosecret",
                10
        )
    }

    override fun logResult(result: SerializationResult<S3StorageConfiguration>?) {
        logger.info("JSON => ${result?.jsonString}")
    }

    override fun logResult(result: YamlSerializationResult<S3StorageConfiguration>?) {
        logger.info("YAML -> ${result?.yamlString}")
    }

    override fun getClazz(): Class<S3StorageConfiguration> {
        return S3StorageConfiguration::class.java
    }

}