package com.shazam.dataengineering.pipelinebuilder;

/**
 * Because Java doesn't have a Tuple
 */
public class S3Environment {
    public final String pipelineName;
    public final String scriptName;

    public S3Environment(String pipelineName, String scriptName) {
        this.pipelineName = pipelineName;
        this.scriptName = scriptName;
    }
}
