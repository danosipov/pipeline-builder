package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.ValidatePipelineDefinitionResult;
import com.amazonaws.services.datapipeline.model.ValidationError;
import com.amazonaws.services.datapipeline.model.ValidationWarning;
import hudson.FilePath;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.Action;
import hudson.model.Run;
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
    private PipelineObject pipelineObject;
    private DeploymentException lastException;
    private List<String> clientMessages = new ArrayList<String>();

    public DeploymentAction(AbstractBuild build, AWSCredentials awsCredentials) {
        this.project = build.getProject();
        this.build = build;
        this.artifacts = build.getArtifacts();
        this.credentials = awsCredentials;
    }

    public String getIconFileName() {
        return "/plugin/pipeline-builder/icons/pipeline-22x22.png";
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

    public String getPipelineFile() {
        return pipelineFile;
    }

    public DeploymentException getLastException() {
        return lastException;
    }

    // TODO: RunOnce
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

    // TODO: Multiple schedule objects per pipeline
    public String getScheduledDate() throws IOException {
        PipelineObject pipelineObject = this.pipelineObject;
        // TODO: Change based on the value of pipeline selector
        if (pipelineObject == null && artifacts.size() > 0) {
            if (pipelineFile != null) {
                pipelineObject = getPipelineByName(pipelineFile);
            } else {
                Run.Artifact artifact = artifacts.get(0); // TODO: Remove this hack
                pipelineObject = new PipelineObject(new FilePath(artifact.getFile()).readToString());
            }
        }
        if (pipelineObject != null) {
            return pipelineObject.getScheduleDate();
        } else {
            return "";
        }
    }

    public void doConfirmProcess(StaplerRequest req, StaplerResponse resp) throws IOException, ServletException {
        // TODO: Process parameters, forward to confirm
        JSONObject formData = req.getSubmittedForm();
        pipelineFile = formData.getString("pipeline");
        String startDate = formData.getString("scheduleDate");
        // TODO: Validate startdate!

        pipelineObject = getPipelineByName(pipelineFile);
        if (pipelineObject == null) {
            clientMessages.add("[ERROR] Pipeline not found");
            resp.forward(this, "error", req);
        } else {
            pipelineObject.setScheduleDate(startDate);
        }

        try {
            DataPipelineClient client = new DataPipelineClient(credentials);
            pipelineToRemoveId = getPipelineId(pipelineFile, client);
        } catch (DeploymentException e) {
            pipelineToRemoveId = "";
        }

        resp.forward(this, "confirm", req);
    }

    public void doDeploy(StaplerRequest req, StaplerResponse resp) throws ServletException, IOException {
        DataPipelineClient client = new DataPipelineClient(credentials);
        try {
            // TODO: Deploy scripts
            String pipelineId = createNewPipeline(client);
            validateNewPipeline(pipelineId, client);
            uploadNewPipeline(pipelineId, client);
            removeOldPipeline(client);
            activateNewPipeline(pipelineId, client);
            resp.forward(this, "report", req);
        } catch (DeploymentException e) {
            if (e.getCause() != null) {
                clientMessages.add("[ERROR] " + e.getCause().getMessage());
            }
            resp.forward(this, "error", req);
        }
    }

    private PipelineObject getPipelineByName(String pipelineName) throws IOException {
        if (!pipelineName.isEmpty() && artifacts != null && artifacts.size() > 0) {
            for (Run.Artifact artifact : artifacts) {
                if (artifact.getFileName().equals(pipelineName)) {
                    return new PipelineObject(new FilePath(artifact.getFile()).readToString());
                }
            }
        }

        return null;
    }

    private void activateNewPipeline(String pipelineId, DataPipelineClient client) throws DeploymentException {
        AWSProxy proxy = new AWSProxy(client);
        proxy.activatePipeline(pipelineId);
        clientMessages.add("[INFO] Pipeline has been activated!");
    }

    private void uploadNewPipeline(String pipelineId, DataPipelineClient client) throws DeploymentException {
        AWSProxy proxy = new AWSProxy(client);
        boolean success = proxy.putPipeline(pipelineId, pipelineObject);
        if (!success) {
            clientMessages.add("[ERROR] Unable to upload new pipeline definition.");
            throw new DeploymentException();
        } else {
            clientMessages.add("[INFO] Upload of pipeline definition completed successfully");
        }
    }

    private void validateNewPipeline(String pipelineId, DataPipelineClient client) throws DeploymentException {
        AWSProxy proxy = new AWSProxy(client);
        ValidatePipelineDefinitionResult validation = proxy.validatePipeline(pipelineId, pipelineObject);

        List<ValidationError> errors = validation.getValidationErrors();
        List<ValidationWarning> warnings = validation.getValidationWarnings();

        for (ValidationError error : errors) {
            for (String errorMessage : error.getErrors()) {
                clientMessages.add("[ERROR] " + errorMessage);
            }
        }

        for (ValidationWarning warning : warnings) {
            for (String warningMessage : warning.getWarnings()) {
                clientMessages.add("[WARN] " + warningMessage);
            }
        }

        if (validation.isErrored()) {
            clientMessages.add("[ERROR] Critical errors detected in validation.");
            throw new DeploymentException();
        } else {
            clientMessages.add("[INFO] No critical errors for the pipeline detected in validation.");
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

    private String getPipelineId(String pipelineName, DataPipelineClient client) throws DeploymentException {
        String pipelineRegex = pipelineName.substring(0, pipelineName.lastIndexOf("-")) + "-\\d+";
        AWSProxy proxy = new AWSProxy(client);
        return proxy.getPipelineId(pipelineRegex);
    }
}
