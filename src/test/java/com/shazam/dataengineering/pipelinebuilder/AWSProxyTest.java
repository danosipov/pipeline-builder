package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.*;
import com.amazonaws.services.datapipeline.model.PipelineObject;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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

        ArgumentCaptor<PutObjectRequest> argument = ArgumentCaptor.forClass(PutObjectRequest.class);

        PutObjectResult putResult = Mockito.mock(PutObjectResult.class);
        Mockito.when(client.putObject(argument.capture())).thenReturn(putResult);

        assertTrue(AWSProxy.uploadFileToS3Url(client, url, file));
        assertEquals(bucketName, argument.getValue().getBucketName());
        assertEquals(key, argument.getValue().getKey());
    }

    @Test
    public void hasRunningTasksShouldReturnTrueForObjectsInRunningState() throws Exception {
        List<String> objectIdList1 = new ArrayList<String>();
        objectIdList1.add("obj1");
        objectIdList1.add("obj2");
        List<String> objectIdList2 = new ArrayList<String>();
        objectIdList2.add("obj3");

        List<PipelineObject> objectList1 = new ArrayList<PipelineObject>();
        objectList1.add(new PipelineObject().withId("obj1").withFields(
                        new Field().withKey("@status").withStringValue("FINISHED"),
                        new Field().withKey("type").withStringValue("EmrCluster")
                ));
        objectList1.add(new PipelineObject().withId("obj2").withFields(
                new Field().withKey("@attemptCount").withStringValue("1"),
                new Field().withKey("@status").withStringValue("FINISHED")
        ));
        List<PipelineObject> objectList2 = new ArrayList<PipelineObject>();
        objectList2.add(new PipelineObject().withId("obj3").withFields(
                new Field().withKey("@status").withStringValue("RUNNING")
        ));

        QueryObjectsResult queryResult1 = Mockito.mock(QueryObjectsResult.class);
        Mockito.when(queryResult1.getIds()).thenReturn(objectIdList1);
        Mockito.when(queryResult1.getHasMoreResults()).thenReturn(true);
        Mockito.when(queryResult1.getMarker()).thenReturn("testMarker");
        QueryObjectsResult queryResult2 = Mockito.mock(QueryObjectsResult.class);
        Mockito.when(queryResult2.getIds()).thenReturn(objectIdList2);
        Mockito.when(queryResult2.getHasMoreResults()).thenReturn(false);

        DataPipelineClient dataPipelineClient = Mockito.mock(DataPipelineClient.class);

        QueryObjectsRequest request1 = new QueryObjectsRequest()
                .withPipelineId("test-pipeline");
        QueryObjectsRequest request2 = new QueryObjectsRequest()
                .withPipelineId("test-pipeline")
                .withMarker("testMarker");
        Mockito.when(dataPipelineClient.queryObjects(request1))
                .thenReturn(queryResult1);
        Mockito.when(dataPipelineClient.queryObjects(request2))
                .thenReturn(queryResult2);

        DescribeObjectsResult describeResult1 = Mockito.mock(DescribeObjectsResult.class);
        Mockito.when(describeResult1.getPipelineObjects()).thenReturn(objectList1);
        DescribeObjectsResult describeResult2 = Mockito.mock(DescribeObjectsResult.class);
        Mockito.when(describeResult2.getPipelineObjects()).thenReturn(objectList2);

        DescribeObjectsRequest describeRequest1 = new DescribeObjectsRequest()
                .withPipelineId("test-pipeline")
                .withObjectIds(objectIdList1);
        DescribeObjectsRequest describeRequest2 = new DescribeObjectsRequest()
                .withPipelineId("test-pipeline")
                .withObjectIds(objectIdList2);
        Mockito.when(dataPipelineClient.describeObjects(describeRequest1))
                .thenReturn(describeResult1);
        Mockito.when(dataPipelineClient.describeObjects(describeRequest2))
                .thenReturn(describeResult2);

        AWSProxy proxy = new AWSProxy(dataPipelineClient);

        assertTrue(proxy.hasRunningTasks("test-pipeline"));
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
