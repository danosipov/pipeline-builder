package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import java.io.File;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Proxy class for the AWS SDK
 * Simplifies most of the interactions, wraps exceptions, helps in testing.
 */
public class AWSProxy {
    private DataPipelineClient client;

    public AWSProxy(DataPipelineClient dataPipelineClient) {
        this.client = dataPipelineClient;
    }

    public static boolean uploadFileToS3Url(AmazonS3 client, String url, File file) throws DeploymentException {
        try {
            Pattern pattern = Pattern.compile("://([^/]+)/(.*)");
            Matcher matcher = pattern.matcher(url);
            if (matcher.find()) {
                String bucketName = matcher.group(1);
                String key = matcher.group(2);
                PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, file);
                PutObjectResult result = client.putObject(putRequest);
                return true;
            } else {
                return false;
            }
        } catch (RuntimeException e) {
            throw new DeploymentException(e);
        }
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
                    .withUniqueId(UUID.randomUUID().toString()); // TODO persist through retries?
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

    public String getPipelineId(String nameRegex) throws DeploymentException {
        return getPipelineId(nameRegex, null);
    }

    public String getPipelineId(String nameRegex, String marker) throws DeploymentException {
        try {
            ListPipelinesRequest request = new ListPipelinesRequest();
            if (marker != null) {
                request.setMarker(marker);
            }

            ListPipelinesResult pipelineList = client.listPipelines(request);
            for (PipelineIdName pipeline : pipelineList.getPipelineIdList()) {
                if (pipeline.getName().matches(nameRegex)) {
                    return pipeline.getId();
                }
            }

            if (pipelineList.getHasMoreResults()) {
                return getPipelineId(nameRegex, pipelineList.getMarker());
            } else {
                return "";
            }
        } catch (RuntimeException e) {
            throw new DeploymentException(e);
        }
    }

}
