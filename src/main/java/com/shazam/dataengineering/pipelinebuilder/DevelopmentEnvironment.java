package com.shazam.dataengineering.pipelinebuilder;

import hudson.Extension;
import org.kohsuke.stapler.DataBoundConstructor;

public class DevelopmentEnvironment extends Environment {
    public DevelopmentEnvironment(String name) {
        super(name);
    }

    @DataBoundConstructor
    public DevelopmentEnvironment(String name, String configParam) {
        super(name, configParam);
    }

    @Extension
    public static final class DescriptorImpl extends EnvironmentDescriptor {
        @Override
        public String getType() {
            return "development";
        }

        @Override
        public String getDisplayName() {
            return "Development";
        }
    }
}
