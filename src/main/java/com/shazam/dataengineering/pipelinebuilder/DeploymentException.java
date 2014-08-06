package com.shazam.dataengineering.pipelinebuilder;

public class DeploymentException extends Exception {
    public DeploymentException() {
        super();
    }

    public DeploymentException(Exception e) {
        super();
        this.initCause(e);
    }
}
