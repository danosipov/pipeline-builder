package com.shazam.dataengineering.pipelinebuilder;

import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.Action;
import hudson.model.Run;
import hudson.util.ListBoxModel;
import org.kohsuke.stapler.StaplerRequest;
import org.kohsuke.stapler.StaplerResponse;

import javax.servlet.ServletException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeploymentAction implements Action {
    private AbstractProject project;
    private AbstractBuild build;
    private List<Run.Artifact> artifacts;

    private String pipeline;
    private String pipeline2;
    private String pipeline3;

    public DeploymentAction(AbstractProject project) {
        this.project = project;
        this.build = project.getLastBuild();
        this.artifacts = build.getArtifacts();
    }

    public String getIconFileName() {
        // TODO
        return "";
    }

    public String getDisplayName() {
        return "Deploy Pipeline";
    }

    public String getUrlName() {
        return "pipeline";
    }

    public List<String> getPipelines() {
        ArrayList<String> pipelines = new ArrayList<String>();
        if (artifacts != null && artifacts.size() > 0) {
            for (Run.Artifact artifact : artifacts) {
                // TODO: Validate the artifact is a valid pipeline
                pipelines.add(artifact.getFileName());
            }
        }

        return pipelines;
    }

    public void doSubmit(StaplerRequest req, StaplerResponse resp) throws IOException, ServletException {

    }
}
