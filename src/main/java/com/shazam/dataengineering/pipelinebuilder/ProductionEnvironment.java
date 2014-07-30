package com.shazam.dataengineering.pipelinebuilder;

import hudson.Extension;
import org.kohsuke.stapler.DataBoundConstructor;

/**
 * Created by daniil.osipov on 7/30/14.
 */
public class ProductionEnvironment extends Environment {
    public ProductionEnvironment(String name) {
        super(name);
    }

    @DataBoundConstructor
    public ProductionEnvironment(String name, String configParam) {
        super(name, configParam);
    }

    @Extension
    public static final class DescriptorImpl extends EnvironmentDescriptor {
        @Override
        public String getType() {
            return "production";
        }

        @Override
        public String getDisplayName() {
            return "Production";
        }
    }
}
