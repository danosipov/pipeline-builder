package com.shazam.dataengineering.pipelinebuilder;

import hudson.DescriptorExtensionList;
import hudson.Extension;
import hudson.model.Describable;
import hudson.model.Descriptor;
import jenkins.model.Jenkins;
import net.sf.json.JSONObject;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.StaplerRequest;

/**
 * Created by daniil.osipov on 7/29/14.
 */
public class Environment implements Describable<Environment> {
    private String name;
    private String properties;

    public Environment(String name) {
        this.name = name;
        this.properties = "key: value";
    }

    @DataBoundConstructor
    public Environment(String name, String configParam) {
        this.name = name;
        this.properties = configParam;
    }

    public EnvironmentDescriptor getDescriptor() {
        //return (EnvironmentDescriptor) Jenkins.getInstance().getDescriptor(getClass());
        return new EnvironmentDescriptor();
    }

    public class EnvironmentDescriptor extends Descriptor<Environment> {
        @Override
        public String getDisplayName() {
            return name;
        }

        public String getName() {
            return name;
        }
    }

}
