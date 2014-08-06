package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.*;
import org.junit.Test;
import org.mockito.Mockito;

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
    public void getPipelineIdShouldReturnEmptyId() throws Exception {
        String result = executeGetPipelineIdMethod("p1-this-is-another-pipeline");

        assertEquals("", result);
    }



    private String executeGetPipelineIdMethod(String regex)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
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

        Mockito.when(dataPipelineClient.listPipelines()).thenReturn(listPipelinesResult);
        Mockito.when(listPipelinesResult.getPipelineIdList()).thenReturn(pipelineList);

        return dataPipelineClient;
    }
}
