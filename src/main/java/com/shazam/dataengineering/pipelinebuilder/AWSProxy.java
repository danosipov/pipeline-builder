package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.*;

import java.util.List;
import java.util.UUID;

public class AWSProxy {
    private DataPipelineClient client;

    public AWSProxy(DataPipelineClient dataPipelineClient) {
        this.client = dataPipelineClient;
    }

    public boolean removePipeline(String pipelineId) {
        try {
            DeletePipelineRequest request = new DeletePipelineRequest().withPipelineId(pipelineId);
            client.deletePipeline(request);

            return true;
        } catch (AmazonClientException e) {
            return false;
        }
    }

    public String createPipeline(String name) throws DeploymentException {
        return createPipeline(name, "");
    }

    public String createPipeline(String name, String description) throws DeploymentException {
        try {
            CreatePipelineRequest request = new CreatePipelineRequest()
                    .withName(name).withDescription(description)
                    .withUniqueId(UUID.randomUUID().toString()); // TODO?
            CreatePipelineResult result = client.createPipeline(request);
            return result.getPipelineId();
        } catch (RuntimeException e) {
            throw new DeploymentException(e);
        }
    }

    public ValidatePipelineDefinitionResult validatePipeline(String pipelineId, PipelineObject pipeline)
            throws DeploymentException {
        try {
            ValidatePipelineDefinitionRequest request = new ValidatePipelineDefinitionRequest()
                    .withPipelineId(pipelineId).withPipelineObjects(pipeline.getAWSObjects());
            return client.validatePipelineDefinition(request);
        } catch (RuntimeException e) {
            throw new DeploymentException(e);
        }
    }

    public boolean putPipeline(String pipelineId, PipelineObject pipeline) throws DeploymentException {
        try {
            PutPipelineDefinitionRequest request = new PutPipelineDefinitionRequest()
                    .withPipelineId(pipelineId).withPipelineObjects(pipeline.getAWSObjects());
            PutPipelineDefinitionResult result = client.putPipelineDefinition(request);
            return !result.isErrored();
        } catch (RuntimeException e) {
            throw new DeploymentException(e);
        }
    }

    public void activatePipeline(String pipelineId) throws DeploymentException {
        try {
            ActivatePipelineRequest request = new ActivatePipelineRequest().withPipelineId(pipelineId);
            client.activatePipeline(request);
        } catch (RuntimeException e) {
            throw new DeploymentException(e);
        }
    }

    public String getPipelineId(String nameRegex) {
        ListPipelinesResult pipelineList = client.listPipelines();
        for (PipelineIdName pipeline: pipelineList.getPipelineIdList()) {
            if (pipeline.getName().matches(nameRegex)) {
                return pipeline.getId();
            }
        }

        return "";
    }

}
