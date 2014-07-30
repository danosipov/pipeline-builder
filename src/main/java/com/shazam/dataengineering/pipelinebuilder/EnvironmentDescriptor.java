package com.shazam.dataengineering.pipelinebuilder;

import hudson.model.Descriptor;

abstract public class EnvironmentDescriptor extends Descriptor<Environment> {
    // define additional constructor parameters if you want
    protected EnvironmentDescriptor(Class<? extends Environment> clazz) {
        super(clazz);
    }

    protected EnvironmentDescriptor() {
    }

    abstract public String getType();
}