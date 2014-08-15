package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AWSProxyTest {
    @Test
    public void createPipelineShouldTriggerPipelineCreation() throws Exception {
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        CreatePipelineResult createPipelineResult = new CreatePipelineResult()
                .withPipelineId("pipelineId123");
        when(dataPipelineClient.createPipeline(any(CreatePipelineRequest.class))).thenReturn(createPipelineResult);
        AWSProxy proxy = new AWSProxy(dataPipelineClient);

        String result = proxy.createPipeline("test");
        assertEquals("pipelineId123", result);
    }

    @Test
    public void removePipelineShouldTriggerPipelineDeletion() throws Exception {
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        AWSProxy proxy = new AWSProxy(dataPipelineClient);

        boolean result = proxy.removePipeline("test1");

        verify(dataPipelineClient).deletePipeline(any(DeletePipelineRequest.class));
        assertTrue(result);
    }

    @Test
    public void removePipelineFailureShouldReturnFalse() throws Exception {
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        doThrow(new AmazonServiceException("FAIL"))
                .when(dataPipelineClient)
                .deletePipeline(any(DeletePipelineRequest.class));

        AWSProxy proxy = new AWSProxy(dataPipelineClient);

        boolean result = proxy.removePipeline("test1");

        assertFalse(result);
    }

    @Test
    public void getPipelineIdShouldReturnCorrectPipeline() throws Exception {
        String result = executeGetPipelineIdMethod("p1-this-is-a-test-pipeline-\\d+");

        assertEquals("test1", result);
    }

    @Test
    public void getPipelineIdShouldReturnCorrectPipelineFromSecondResultPage() throws Exception {
        List<PipelineIdName> pipelineList1 = new ArrayList<PipelineIdName>();
        pipelineList1.add(new PipelineIdName().withId("test1").withName("p1-this-is-a-test-pipeline-1"));
        pipelineList1.add(new PipelineIdName().withId("test2").withName("d2-this-is-a-test-pipeline-1"));
        List<PipelineIdName> pipelineList2 = new ArrayList<PipelineIdName>();
        pipelineList2.add(new PipelineIdName().withId("test3").withName("p1-test-pipeline-2"));
        pipelineList2.add(new PipelineIdName().withId("test4").withName("d2-test-pipeline-2"));

        ListPipelinesResult listPipelinesResult1 = Mockito.mock(ListPipelinesResult.class);
        Mockito.when(listPipelinesResult1.getPipelineIdList()).thenReturn(pipelineList1);
        Mockito.when(listPipelinesResult1.getHasMoreResults()).thenReturn(true);
        Mockito.when(listPipelinesResult1.getMarker()).thenReturn("testMarker");
        ListPipelinesResult listPipelinesResult2 = Mockito.mock(ListPipelinesResult.class);
        Mockito.when(listPipelinesResult2.getPipelineIdList()).thenReturn(pipelineList2);
        Mockito.when(listPipelinesResult2.getHasMoreResults()).thenReturn(false);
        DataPipelineClient dataPipelineClient = Mockito.mock(DataPipelineClient.class);

        ListPipelinesRequest request1 = new ListPipelinesRequest();
        ListPipelinesRequest request2 = new ListPipelinesRequest().withMarker("testMarker");
        Mockito.when(dataPipelineClient.listPipelines(request1))
                .thenReturn(listPipelinesResult1);
        Mockito.when(dataPipelineClient.listPipelines(request2))
                .thenReturn(listPipelinesResult2);

        AWSProxy proxy = new AWSProxy(dataPipelineClient);

        String result = proxy.getPipelineId(("p1-test-pipeline-\\d+"));

        assertEquals("test3", result);
    }

    @Test
    public void getPipelineIdShouldReturnEmptyIdForMissingPipeline() throws Exception {
        String result = executeGetPipelineIdMethod("p1-this-is-another-pipeline");

        assertEquals("", result);
    }

    @Test
    public void uploadFileToS3UrlShouldReturnTrueForSuccessfulUpload() throws Exception {
        AmazonS3 client = Mockito.mock(AmazonS3.class);

        String bucketName = "test-bucket";
        String key = "test/key/file.name";
        String url = "s3://" + bucketName + "/" + key;
        File file = Mockito.mock(File.class);

        PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, file);
        PutObjectResult putResult = Mockito.mock(PutObjectResult.class);
        Mockito.when(client.putObject(putRequest)).thenReturn(putResult);

        assertTrue(AWSProxy.uploadFileToS3Url(client, url, file));
    }



    private String executeGetPipelineIdMethod(String regex)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, DeploymentException {
        List<PipelineIdName> pipelineList = new ArrayList<PipelineIdName>();
        pipelineList.add(new PipelineIdName().withId("test1").withName("p1-this-is-a-test-pipeline-1"));
        pipelineList.add(new PipelineIdName().withId("test2").withName("d2-this-is-a-test-pipeline-1"));
        DataPipelineClient client = getMockDataPipelineClient(pipelineList);
        AWSProxy proxy = new AWSProxy(client);

        return proxy.getPipelineId(regex);
    }

    private DataPipelineClient getMockDataPipelineClient(List<PipelineIdName> pipelineList) {
        ListPipelinesResult listPipelinesResult = Mockito.mock(ListPipelinesResult.class);
        DataPipelineClient dataPipelineClient = Mockito.mock(DataPipelineClient.class);

        Mockito.when(dataPipelineClient.listPipelines(any(ListPipelinesRequest.class)))
                .thenReturn(listPipelinesResult);
        Mockito.when(listPipelinesResult.getPipelineIdList()).thenReturn(pipelineList);

        return dataPipelineClient;
    }
}
