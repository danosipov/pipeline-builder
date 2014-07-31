package com.shazam.dataengineering.pipelinebuilder;

import hudson.model.AbstractBuild;
import hudson.model.BuildListener;
import hudson.model.Result;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.WithoutJenkins;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Map;

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
        AbstractBuild build = Mockito.mock(AbstractBuild.class);
        PipelineProcessor processor = new PipelineProcessor(build, listener);

        Method method = processor.getClass().getDeclaredMethod("getSubstitutionMap", Environment.class);
        method.setAccessible(true);

        Environment env = new Environment("test", "key1: value1\nkey2: value2\n$key3: $value3");

        Map<String, String> result = (Map<String, String>) method.invoke(processor, env);

        Assert.assertEquals("value1", result.get("key1"));
        Assert.assertEquals("value2", result.get("key2"));
        Assert.assertEquals("$value3", result.get("$key3"));
        Assert.assertFalse(result.containsKey("notThere"));
    }

    @Test
    @WithoutJenkins
    public void performSubstitutionsShouldSubstitutePlaceholders() throws Exception {
        BuildListener listener = Mockito.mock(BuildListener.class);
        AbstractBuild build = Mockito.mock(AbstractBuild.class);
        PipelineProcessor processor = new PipelineProcessor(build, listener);

        Method method = processor.getClass().getDeclaredMethod("performSubstitutions", String.class, Environment.class);
        method.setAccessible(true);

        Environment env = new Environment("test", "key1: value1\nkey2: value2\n$key3: $value3");
        String json = "{\"object1\":\"${key1}\", \"object2\":\"${$key3}\", \"object3\":\"${key4}\"}";
        String expected = "{\"object1\":\"value1\", \"object2\":\"$value3\", \"object3\":\"${key4}\"}";

        String result = (String) method.invoke(processor, json, env);
        Assert.assertEquals(expected, result);
    }
}
