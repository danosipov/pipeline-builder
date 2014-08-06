package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.*;

import java.util.List;

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
                    .withName(name).withDescription(description);
            CreatePipelineResult result = client.createPipeline(request);
            return result.getPipelineId();
        } catch (AmazonClientException e) {
            throw new DeploymentException(e);
        }
    }

//    public List<String> validatePipeline(String pipelineId) throws DeploymentException {
//        ValidatePipelineDefinitionRequest request = new ValidatePipelineDefinitionRequest()
//                .withPipelineId(pipelineId).withPipelineObjects()
//        client.validatePipelineDefinition()
//    }

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
