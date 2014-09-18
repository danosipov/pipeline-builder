package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.ValidatePipelineDefinitionResult;
import com.amazonaws.services.datapipeline.model.ValidationError;
import com.amazonaws.services.datapipeline.model.ValidationWarning;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import hudson.FilePath;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.Action;
import hudson.model.Run;
import net.sf.json.JSONObject;
import org.kohsuke.stapler.StaplerRequest;
import org.kohsuke.stapler.StaplerResponse;

import javax.servlet.ServletException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DeploymentAction implements Action {
    private AbstractProject project;
    private AbstractBuild build;
    private Map<S3Environment, String> s3Urls;
    private List<Run.Artifact> artifacts;
    private AWSCredentials credentials;

    private String pipelineToRemoveId;
    private String pipelineFile;
    private PipelineObject pipelineObject;
    private DeploymentException lastException;
    private List<String> clientMessages = new ArrayList<String>();

    public DeploymentAction(AbstractBuild build, Map<S3Environment, String> s3Urls, AWSCredentials awsCredentials) {
        this.project = build.getProject();
        this.build = build;
        this.s3Urls = s3Urls;
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

    public boolean hasPipelineToRemove() {
        return pipelineToRemoveId != null && !pipelineToRemoveId.isEmpty();
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

    public boolean isStartDatePast() {
        return pipelineObject != null && PipelineObject.isPast(pipelineObject.getScheduleDate());
    }

    public boolean hasScriptsToDeploy() {
        return s3Urls.size() > 0;
    }

    public boolean oldPipelineHasRunningTasks() {
        DataPipelineClient client = new DataPipelineClient(credentials);
        AWSProxy proxy = new AWSProxy(client);

        return proxy.hasRunningTasks(pipelineToRemoveId);
    }

    public List<String> getPipelines() {
        ArrayList<String> pipelines = new ArrayList<String>();
        if (artifacts != null && artifacts.size() > 0) {
            for (Run.Artifact artifact : artifacts) {
                try {
                    PipelineObject object = new PipelineObject(new FilePath(artifact.getFile()).readToString());
                    if (object.isValid()) {
                        pipelines.add(artifact.getFileName());
                    }
                } catch (IOException e) {
                    // Ignore
                }
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
                Run.Artifact artifact = null;
                // Get the pipeline file, any pipeline file
                // TODO: This makes a lot of assumptions, need to fix.
                for (Run.Artifact allegedPipeline : artifacts) {
                    if (allegedPipeline.getFileName().endsWith(".json")) {
                        artifact = allegedPipeline;
                    }
                }

                if (artifact != null) {
                    pipelineObject = new PipelineObject(
                            new FilePath(artifact.getFile()).readToString());
                }
            }
        }

        if (pipelineObject != null) {
            return pipelineObject.getScheduleDate();
        } else {
            return "";
        }
    }

    public void doConfirmProcess(StaplerRequest req, StaplerResponse resp) throws IOException, ServletException {
        // Clear out previous warnings
        clientMessages.clear();

        JSONObject formData = req.getSubmittedForm();
        pipelineFile = formData.getString("pipeline");
        String startDate = formData.getString("scheduleDate");

        // Validate start date, and warn if its in the past.
        if (!PipelineObject.validateDate(startDate)) {
            clientMessages.add("[ERROR] Passed start date was not in expected format: " + PipelineObject.PIPELINE_DATE_FORMAT);
            resp.forward(this, "error", req);
        } else if (PipelineObject.isPast(startDate)) {
            clientMessages.add("[WARN] Passed start date is in the past. Backfill may occur.");
        }

        // Validate chosen pipeline
        pipelineObject = getPipelineByName(pipelineFile);
        if (pipelineObject == null) {
            clientMessages.add("[ERROR] Pipeline not found");
            resp.forward(this, "error", req);
        } else {
            pipelineObject.setScheduleDate(startDate);
        }

        // Find previously deployed pipeline.
        try {
            DataPipelineClient client = new DataPipelineClient(credentials);

            pipelineToRemoveId = getPipelineId(pipelineFile, client);
            if (oldPipelineHasRunningTasks()) {
                clientMessages.add("[WARN] Old pipeline is currently running. Execution will be terminated.");
            }
        } catch (DeploymentException e) {
            pipelineToRemoveId = "";
        }

        resp.forward(this, "confirm", req);
    }

    public void doDeploy(StaplerRequest req, StaplerResponse resp) throws ServletException, IOException {
        DataPipelineClient client = new DataPipelineClient(credentials);
        try {
            String pipelineId = createNewPipeline(client);
            validateNewPipeline(pipelineId, client);
            uploadNewPipeline(pipelineId, client);
            deployScriptsToS3();
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

    private void deployScriptsToS3() throws DeploymentException {
        String pathPrefix = build.getArtifactsDir().getPath() + "/scripts/";
        AmazonS3 s3Client = new AmazonS3Client(credentials);
        for (S3Environment env : s3Urls.keySet()) {
            if (env.pipelineName.equals(pipelineFile)) {
                String filename = env.scriptName;
                File file = new File(pathPrefix + filename);
                if (file.exists()) {
                    String url = s3Urls.get(filename);
                    clientMessages.add(String.format("[INFO] Uploading %s to %s", filename, url));
                    boolean result = AWSProxy.uploadFileToS3Url(s3Client, url, file);
                    if (result) {
                        clientMessages.add(String.format("[INFO] Upload successful!"));
                    } else {
                        clientMessages.add(String.format("[ERROR] Upload failed!"));
                    }
                } else {
                    clientMessages.add(String.format("[ERROR] Unable to find %s in artifacts", filename));
                }
            }
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

    private String getPipelineName() {
        return pipelineFile.substring(0, pipelineFile.lastIndexOf(".json"));
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
        AWSProxy proxy = new AWSProxy(client);
        return proxy.createPipeline(getPipelineName());
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
