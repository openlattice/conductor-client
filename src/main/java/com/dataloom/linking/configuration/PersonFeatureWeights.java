package com.dataloom.linking.configuration;

import com.dataloom.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration;

@ReloadableConfiguration(
    uri = "personFeatureWeights.yaml" )
public class PersonFeatureWeights {

    private double[] weights;

    @JsonCreator
    public PersonFeatureWeights(
            @JsonProperty( SerializationConstants.BIAS ) double bias,
            @JsonProperty( SerializationConstants.FIRST_NAME_STRING ) double firstNameString,
            @JsonProperty( SerializationConstants.FIRST_NAME_METAPHONE ) double firstNameMetaphone,
            @JsonProperty( SerializationConstants.FIRST_NAME_PRESENCE ) double firstNamePresence,
            @JsonProperty( SerializationConstants.MIDDLE_NAME_STRING ) double middleNameString,
            @JsonProperty( SerializationConstants.MIDDLE_NAME_METAPHONE ) double middleNameMetaphone,
            @JsonProperty( SerializationConstants.MIDDLE_NAME_PRESENCE ) double middleNamePresence,
            @JsonProperty( SerializationConstants.LAST_NAME_STRING ) double lastNameString,
            @JsonProperty( SerializationConstants.LAST_NAME_METAPHONE ) double lastNameMetaphone,
            @JsonProperty( SerializationConstants.LAST_NAME_PRESENCE ) double lastNamePresence,
            @JsonProperty( SerializationConstants.SEX_STRING ) double sexString,
            @JsonProperty( SerializationConstants.SEX_PRESENCE ) double sexPresence,
            @JsonProperty( SerializationConstants.RACE_STRING ) double raceString,
            @JsonProperty( SerializationConstants.RACE_PRESENCE ) double racePresence,
            @JsonProperty( SerializationConstants.ETHNICITY_STRING ) double ethnicityString,
            @JsonProperty( SerializationConstants.ETHNICITY_PRESENCE ) double ethnicityPresence,
            @JsonProperty( SerializationConstants.DOB_STRING ) double dobString,
            @JsonProperty( SerializationConstants.DOB_PRESENCE ) double dobPresence,
            @JsonProperty( SerializationConstants.IDENTIFICATION_STRING ) double identificationString,
            @JsonProperty( SerializationConstants.IDENTIFICATION_PRESENCE ) double identificationPresence,
            @JsonProperty( SerializationConstants.SSN_STRING ) double ssnString,
            @JsonProperty( SerializationConstants.SSN_PRESENCE ) double ssnPresence,
            @JsonProperty( SerializationConstants.AGE_STRING ) double ageString,
            @JsonProperty( SerializationConstants.AGE_PRESENCE ) double agePresence,
            @JsonProperty( SerializationConstants.XREF_STRING ) double xrefString,
            @JsonProperty( SerializationConstants.XREF_PRESENCE ) double xrefPresence ) {
        this.weights = new double[] { bias, firstNameString, firstNameMetaphone, firstNamePresence, middleNameString,
                middleNameMetaphone, middleNamePresence, lastNameString, lastNameMetaphone, lastNamePresence, sexString,
                sexPresence, raceString, racePresence, ethnicityString, ethnicityPresence, dobString, dobPresence,
                identificationString, identificationPresence, ssnString, ssnPresence, ageString, agePresence,
                xrefString, xrefPresence };
    }

    @JsonProperty( SerializationConstants.WEIGHTS )
    public double[] getWeights() {
        return weights;
    }

}
