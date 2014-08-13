package com.shazam.dataengineering.pipelinebuilder;

import hudson.Extension;
import hudson.model.Job;
import hudson.views.ListViewColumn;
import hudson.views.ListViewColumnDescriptor;
import org.kohsuke.stapler.DataBoundConstructor;

public class ReleaseButtonColumn extends ListViewColumn {
    @DataBoundConstructor
    public ReleaseButtonColumn() {
    }

    public boolean isDeployConfigured(Job job) {
        return job.getLastBuild() != null && job.getLastBuild().getAction(DeploymentAction.class) != null;
    }

    @Extension
    public static class DescriptorImpl extends ListViewColumnDescriptor {

        @Override
        public boolean shownByDefault() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Deploy AWS Pipeline";
        }

    }
}
