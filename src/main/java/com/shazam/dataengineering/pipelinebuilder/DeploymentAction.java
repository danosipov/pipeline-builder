package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.DeletePipelineRequest;
import com.amazonaws.services.datapipeline.model.ListPipelinesResult;
import com.amazonaws.services.datapipeline.model.PipelineIdName;
import hudson.FilePath;
import hudson.model.*;
import net.sf.json.JSONObject;
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
    private AWSCredentials credentials;

    private String pipelineToRemoveId;
    private String pipelineFile;
    private List<String> clientMessages = new ArrayList<String>();

    public DeploymentAction(AbstractBuild build, AWSCredentials awsCredentials) {
        this.project = build.getProject();
        this.build = build;
        this.artifacts = build.getArtifacts();
        this.credentials = awsCredentials;
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

    public AbstractBuild getBuild() {
        return build;
    }

    public String getPipelineToRemoveId() {
        return pipelineToRemoveId;
    }

    public List<String> getClientMessages() {
        return clientMessages;
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

    public String getScheduledDate() throws IOException {
        PipelineObject pipelineObject = null;
        // TODO: Change based on the value of pipeline selector
        if (artifacts.size() > 0) {
            Run.Artifact artifact = artifacts.get(0); // TODO: Remove this hack
            pipelineObject = new PipelineObject(new FilePath(artifact.getFile()).readToString());
        }
        if (pipelineObject != null) {
            return pipelineObject.getScheduleDate();
        } else {
            return "";
        }
    }

    public void doSubmit(StaplerRequest req, StaplerResponse resp) throws IOException, ServletException {

        // TODO: Process, display confirmation
        if (req.getAttribute("confirmation") == null) {
            JSONObject formData = req.getSubmittedForm();
            pipelineFile = formData.getString("pipeline");

            DataPipelineClient client = new DataPipelineClient(credentials);

            // TODO: check what step we're on

            req.setAttribute("confirmation", false);
            req.setAttribute("remove-pipeline-id", getPipelineId(pipelineFile, client));

            resp.forward(build, "pipeline", req);
        } else if (!((Boolean) req.getAttribute("confirmation"))) {
            // TODO: Show confirmation page
        } else {
            // TODO: Perform actions
        }

    }

    public void doConfirmProcess(StaplerRequest req, StaplerResponse resp) throws IOException, ServletException {
        // TODO: Process parameters, forward to confirm
        JSONObject formData = req.getSubmittedForm();
        String pipelineName = formData.getString("pipeline");

        DataPipelineClient client = new DataPipelineClient(credentials);
        pipelineToRemoveId = getPipelineId(pipelineName, client);

        resp.forward(this, "confirm", req);
    }

    public void doDeploy(StaplerRequest req, StaplerResponse resp) throws ServletException, IOException {
        DataPipelineClient client = new DataPipelineClient(credentials);
        // TODO: Deployment!
        try {
            removeOldPipeline(client);
            String pipelineId = createNewPipeline(client);
            //validateNewPipeline(pipelineId, client);
            //uploadNewPipeline(pipelineId, client);
            //activateNewPipeline(pipelineId, client);
        } catch (DeploymentException e) {
            // TODO: Render error for the user
        } finally {
            resp.forward(this, "report", req);
        }

    }

    private String createNewPipeline(DataPipelineClient client) throws DeploymentException {
        String pipelineName = pipelineFile.substring(0, pipelineFile.lastIndexOf(".json"));
        AWSProxy proxy = new AWSProxy(client);
        return proxy.createPipeline(pipelineName);
    }

    private void removeOldPipeline(DataPipelineClient client) throws DeploymentException {
        if (pipelineToRemoveId == null || !pipelineToRemoveId.isEmpty()) {
            AWSProxy proxy = new AWSProxy(client);
            boolean result = proxy.removePipeline(pipelineToRemoveId);

            if (result) {
                clientMessages.add("[INFO] Successfully removed pipeline " + pipelineToRemoveId);
            } else {
                clientMessages.add("[WARN] Failed to remove pipeline " + pipelineToRemoveId);
            }
        } else {
            clientMessages.add("[INFO] No old pipeline to remove");
        }
    }

    private String getPipelineId(String pipelineName, DataPipelineClient client) {
        String pipelineRegex = pipelineName.substring(0, pipelineName.lastIndexOf("-")) + "-\\d+";
        AWSProxy proxy = new AWSProxy(client);
        return proxy.getPipelineId(pipelineRegex);
    }
}
