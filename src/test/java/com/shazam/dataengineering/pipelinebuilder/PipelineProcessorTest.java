package com.shazam.dataengineering.pipelinebuilder;

import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.BuildListener;
import hudson.model.Result;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.WithoutJenkins;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.Assert.*;

public class PipelineProcessorTest {
    @Rule
    public JenkinsRule jenkins = new JenkinsRule();

    @Test
    @WithoutJenkins
    public void pipelineProcessorShouldSubstituteCorrectly() throws Exception {
        AbstractBuild build = Mockito.mock(AbstractBuild.class);
        Mockito.when(build.getResult()).thenReturn(Result.FAILURE);
    }

    @Test
    @WithoutJenkins
    public void getSubstitutionMapShouldConvertEnvironmentParametersToMap() throws Exception {
        BuildListener listener = Mockito.mock(BuildListener.class);
        PipelineProcessor processor = new PipelineProcessor(getMockAbstractBuild(), listener);

        Method method = processor.getClass().getDeclaredMethod("getSubstitutionMap", Environment.class);
        method.setAccessible(true);

        Environment env = new Environment("test", "key1: value1\nkey2: value2\n$key3: $value3");

        Map<String, String> result = (Map<String, String>) method.invoke(processor, env);

        assertEquals("value1", result.get("key1"));
        assertEquals("value2", result.get("key2"));
        assertEquals("$value3", result.get("$key3"));
        assertFalse(result.containsKey("notThere"));
    }

    @Test
    @WithoutJenkins
    public void performSubstitutionsShouldSubstitutePlaceholders() throws Exception {
        BuildListener listener = Mockito.mock(BuildListener.class);
        PipelineProcessor processor = new PipelineProcessor(getMockAbstractBuild(), listener);

        Method method = processor.getClass().getDeclaredMethod("performSubstitutions", String.class, Environment.class);
        method.setAccessible(true);

        Environment env = new Environment("test", "key1: value1\nkey2: value2\n$key3: $value3");
        String json = "{\"object1\":\"${key1}\", \"object2\":\"${$key3}\", \"object3\":\"${key4}\"}";
        String expected = "{\"object1\":\"value1\", \"object2\":\"$value3\", \"object3\":\"${key4}\"}";

        String result = (String) method.invoke(processor, json, env);
        assertEquals(expected, result);
    }

    private AbstractBuild getMockAbstractBuild() {
        AbstractBuild build = Mockito.mock(AbstractBuild.class);
        AbstractProject project = Mockito.mock(AbstractProject.class);
        Mockito.when(build.getProject()).thenReturn(project);
        Mockito.when(project.getName()).thenReturn("test");
        Mockito.when(build.getNumber()).thenReturn(42);
        return build;
    }
}
