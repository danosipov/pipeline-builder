package com.shazam.dataengineering.pipelinebuilder;


import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.datapipeline.DataPipelineClient;
import com.amazonaws.services.datapipeline.model.*;
import hudson.FilePath;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.Run;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test suite for the DeploymentAction class.
 *
 * Tests a lot of private methods, which is a bad practice, but
 * necessary to inject a mocked AWS client in this case, as well
 * as testing individual deployment pieces rather than walking
 * through the whole deployment in one step.
 */
public class DeploymentActionTest {
    @Test
    public void getPipelineIdShouldReturnCorrectPipeline() throws Exception {
        String result = executeGetPipelineIdMethod("p1-this-is-a-test-pipeline-2");

        assertEquals("test1", result);
    }

    @Test
    public void getPipelineIdShouldReturnEmptyId() throws Exception {
        String result = executeGetPipelineIdMethod("p1-this-is-another-pipeline-2");

        assertEquals("", result);
    }
    
    @Test
    public void removeOldPipelineShouldGenerateInfoMessagesForSuccess() throws Exception {
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        DeploymentAction action = new DeploymentAction(getMockAbstractBuild(), new AnonymousAWSCredentials());
        DeletePipelineRequest request = new DeletePipelineRequest().withPipelineId("test");

        Field pipelineIdField = action.getClass().getDeclaredField("pipelineToRemoveId");
        pipelineIdField.setAccessible(true);
        pipelineIdField.set(action, "test");
        Method method = action.getClass().getDeclaredMethod("removeOldPipeline", DataPipelineClient.class);
        method.setAccessible(true);

        method.invoke(action, dataPipelineClient);
        verify(dataPipelineClient).deletePipeline(request);
        assertTrue(action.getClientMessages().get(0).contains("[INFO]"));
        assertFalse(action.getClientMessages().get(0).contains("[WARN]"));
    }

    @Test
    public void createNewPipelineShouldReturnPipelineId() throws Exception {
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        DeploymentAction action = new DeploymentAction(getMockAbstractBuild(), new AnonymousAWSCredentials());
        CreatePipelineResult createPipelineResult = new CreatePipelineResult().withPipelineId("test12345");
        when(dataPipelineClient.createPipeline(any(CreatePipelineRequest.class))).thenReturn(createPipelineResult);

        Field pipelineFileField = action.getClass().getDeclaredField("pipelineFile");
        pipelineFileField.setAccessible(true);
        pipelineFileField.set(action, "p1-test-pipeline-name-34.json");
        Method method = action.getClass().getDeclaredMethod("createNewPipeline", DataPipelineClient.class);
        method.setAccessible(true);

        String result = (String) method.invoke(action, dataPipelineClient);

        assertEquals("test12345", result);
    }

    @Test
    public void validateNewPipelineShouldSaveWarningAndErrorMessages() throws Exception{
        String pipelineId = "test1234";
        String json = new FilePath(new File("src/test/resources/pipeline3.json")).readToString();
        PipelineObject pipeline = new PipelineObject(json);

        ValidatePipelineDefinitionRequest validationRequest = new ValidatePipelineDefinitionRequest()
                .withPipelineId(pipelineId).withPipelineObjects(pipeline.getAWSObjects());
        ValidatePipelineDefinitionResult validationResponse = new ValidatePipelineDefinitionResult()
                .withValidationWarnings(
                        new ValidationWarning().withWarnings("1", "2", "3")
                )
                .withValidationErrors(
                        new ValidationError().withErrors("4", "5"),
                        new ValidationError().withErrors("6")
                ).withErrored(false);
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        when(dataPipelineClient.validatePipelineDefinition(validationRequest)).thenReturn(validationResponse);

        DeploymentAction action = new DeploymentAction(getMockAbstractBuild(), new AnonymousAWSCredentials());

        Field pipelineFileField = action.getClass().getDeclaredField("pipelineObject");
        pipelineFileField.setAccessible(true);
        pipelineFileField.set(action, pipeline);
        Method method = action.getClass().getDeclaredMethod("validateNewPipeline", String.class, DataPipelineClient.class);
        method.setAccessible(true);

        method.invoke(action, pipelineId, dataPipelineClient);

        assertEquals(7, action.getClientMessages().size());
        assertTrue(action.getClientMessages().get(0).contains("[ERROR]"));
        assertTrue(action.getClientMessages().get(1).contains("[ERROR]"));
        assertTrue(action.getClientMessages().get(2).contains("[ERROR]"));
        assertTrue(action.getClientMessages().get(3).contains("[WARN]"));
        assertTrue(action.getClientMessages().get(4).contains("[WARN]"));
        assertTrue(action.getClientMessages().get(5).contains("[WARN]"));
    }

    @Test(expected = InvocationTargetException.class) // Caused by a DeploymentException
    public void validateNewPipelineShouldThrowExceptionWhenValidationFails() throws Exception {
        String pipelineId = "test1234";
        ArrayList<com.amazonaws.services.datapipeline.model.PipelineObject> pipelineList =
                new ArrayList<com.amazonaws.services.datapipeline.model.PipelineObject>();
        PipelineObject pipeline = mock(PipelineObject.class);
        when(pipeline.getAWSObjects()).thenReturn(pipelineList);

        ValidatePipelineDefinitionRequest validationRequest = new ValidatePipelineDefinitionRequest()
                .withPipelineId(pipelineId).withPipelineObjects(pipelineList);
        ValidatePipelineDefinitionResult validationResponse = new ValidatePipelineDefinitionResult()
                .withValidationWarnings(
                        new ValidationWarning().withWarnings("1", "2", "3")
                )
                .withValidationErrors(
                        new ValidationError().withErrors("4", "5"),
                        new ValidationError().withErrors("6")
                ).withErrored(true);
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        when(dataPipelineClient.validatePipelineDefinition(validationRequest)).thenReturn(validationResponse);

        DeploymentAction action = new DeploymentAction(getMockAbstractBuild(), new AnonymousAWSCredentials());

        Field pipelineFileField = action.getClass().getDeclaredField("pipelineObject");
        pipelineFileField.setAccessible(true);
        pipelineFileField.set(action, pipeline);
        Method method = action.getClass().getDeclaredMethod("validateNewPipeline", String.class, DataPipelineClient.class);
        method.setAccessible(true);

        method.invoke(action, pipelineId, dataPipelineClient);
    }

    @Test
    public void uploadNewPipelineShouldCallPutPipeline() throws Exception {
        String pipelineId = "test1234";
        ArrayList<com.amazonaws.services.datapipeline.model.PipelineObject> pipelineList =
                new ArrayList<com.amazonaws.services.datapipeline.model.PipelineObject>();
        PipelineObject pipeline = mock(PipelineObject.class);
        when(pipeline.getAWSObjects()).thenReturn(pipelineList);

        PutPipelineDefinitionRequest putRequest = new PutPipelineDefinitionRequest()
                .withPipelineId(pipelineId).withPipelineObjects(pipelineList);
        PutPipelineDefinitionResult putResult = new PutPipelineDefinitionResult().withErrored(false);
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        when(dataPipelineClient.putPipelineDefinition(putRequest)).thenReturn(putResult);

        DeploymentAction action = new DeploymentAction(getMockAbstractBuild(), new AnonymousAWSCredentials());

        Field pipelineFileField = action.getClass().getDeclaredField("pipelineObject");
        pipelineFileField.setAccessible(true);
        pipelineFileField.set(action, pipeline);
        Method method = action.getClass().getDeclaredMethod("uploadNewPipeline", String.class, DataPipelineClient.class);
        method.setAccessible(true);

        method.invoke(action, pipelineId, dataPipelineClient);

        verify(pipeline).getAWSObjects();
        verify(dataPipelineClient).putPipelineDefinition(any(PutPipelineDefinitionRequest.class));
    }

    @Test
    public void activateNewPipelineShouldCallActivatePipeline() throws Exception {
        String pipelineId = "test1234";
        ActivatePipelineRequest activateRequest = new ActivatePipelineRequest()
                .withPipelineId(pipelineId);
        ActivatePipelineResult activateResult = new ActivatePipelineResult();
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);
        when(dataPipelineClient.activatePipeline(activateRequest)).thenReturn(activateResult);

        DeploymentAction action = new DeploymentAction(getMockAbstractBuild(), new AnonymousAWSCredentials());

        Method method = action.getClass().getDeclaredMethod("activateNewPipeline", String.class, DataPipelineClient.class);
        method.setAccessible(true);

        method.invoke(action, pipelineId, dataPipelineClient);

        verify(dataPipelineClient).activatePipeline(any(ActivatePipelineRequest.class));
    }


    private String executeGetPipelineIdMethod(String pipelineFileName)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        List<PipelineIdName> pipelineList = new ArrayList<PipelineIdName>();
        pipelineList.add(new PipelineIdName().withId("test1").withName("p1-this-is-a-test-pipeline-1"));
        pipelineList.add(new PipelineIdName().withId("test2").withName("d2-this-is-a-test-pipeline-1"));
        DeploymentAction action = new DeploymentAction(getMockAbstractBuild(), new AnonymousAWSCredentials());
        DataPipelineClient client = getMockDataPipelineClient(pipelineList);

        Method method = action.getClass().getDeclaredMethod("getPipelineId", String.class, DataPipelineClient.class);
        method.setAccessible(true);

        return (String) method.invoke(action, pipelineFileName, client);
    }

    private DataPipelineClient getMockDataPipelineClient(List<PipelineIdName> pipelineList) {
        ListPipelinesResult listPipelinesResult = mock(ListPipelinesResult.class);
        DataPipelineClient dataPipelineClient = mock(DataPipelineClient.class);

        when(dataPipelineClient.listPipelines()).thenReturn(listPipelinesResult);
        when(listPipelinesResult.getPipelineIdList()).thenReturn(pipelineList);

        return dataPipelineClient;
    }

    private AbstractBuild getMockAbstractBuild() {
        AbstractBuild build = mock(AbstractBuild.class);
        AbstractProject project = mock(AbstractProject.class);

        when(build.getProject()).thenReturn(project);
        when(project.getName()).thenReturn("test");
        when(build.getArtifacts()).thenReturn(new ArrayList< Run.Artifact>());

        return build;
    }
}
