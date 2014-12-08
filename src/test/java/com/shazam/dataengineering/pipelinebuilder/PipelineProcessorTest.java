package com.shazam.dataengineering.pipelinebuilder;

import hudson.Launcher;
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
import java.util.List;
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
        PipelineProcessor processor = getDefaultPipelineProcessor();

        Method method = processor.getClass().getDeclaredMethod("getSubstitutionMap", Environment.class);
        method.setAccessible(true);

        Environment env = new Environment("test", "key1: value1\nkey2: value2a:value2b\n$key3: $value3");

        Map<String, String> result = (Map<String, String>) method.invoke(processor, env);

        assertEquals("value1", result.get("key1"));
        assertEquals("value2a:value2b", result.get("key2"));
        assertEquals("$value3", result.get("$key3"));
        assertFalse(result.containsKey("notThere"));
    }

    @Test
    @WithoutJenkins
    public void performSubstitutionsShouldSubstitutePlaceholders() throws Exception {
        PipelineProcessor processor = getDefaultPipelineProcessor();

        Method method = processor.getClass().getDeclaredMethod("performSubstitutions",
                String.class, String.class, Environment.class);
        method.setAccessible(true);

        Environment env = new Environment("test", "key1: value1\nkey2: value2\n$key3: $value3");
        String json = "{\"object1\":\"${key1}\", \"object2\":\"${$key3}\", \"object3\":\"${key4}\"}";
        String expected = "{\"object1\":\"value1\", \"object2\":\"$value3\", \"object3\":\"${key4}\"}";

        String result = (String) method.invoke(processor, json, "", env);
        assertEquals(expected, result);
    }

    @Test
    @WithoutJenkins
    public void unreplacedKeysShouldGenerateAWarning() throws Exception {
        PipelineProcessor processor = getDefaultPipelineProcessor();
        String json = "{\"object1\":\"${key1}\", \"object2\":\"${$key3}\", \"object3\":\"${key4}\"}";

        Method method = processor.getClass().getDeclaredMethod("warnForUnreplacedKeys", String.class);
        method.setAccessible(true);

        List<String> warnings = (List<String>) method.invoke(processor, json);

        assertEquals(3, warnings.size());
        assertEquals("Unreplaced token found in pipeline object: ${key1}", warnings.get(0));
    }

    @Test
    @WithoutJenkins
    public void inliningShouldProduceValidJson() throws Exception {
        PipelineProcessor processor = getDefaultPipelineProcessor();
        String json = "{\"inlineThis\":\"\"\"multi\nline\ntext\"\"\"}";
        String expected = "{\"inlineThis\":\"multilinetext\"}";

        Method method = processor.getClass().getDeclaredMethod("performInlining", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(processor, json);
        assertEquals(expected, result);

        PipelineObject pipelineObject = new PipelineObject(result);
        assertTrue(pipelineObject.isValid());
    }

    @Test
    @WithoutJenkins
    public void inliningWithVariablesShouldProduceValidJson() throws Exception {
        PipelineProcessor processor = getDefaultPipelineProcessor();
        String json = "{\"inlineThis\":\"\"\"multi\nl${in}e\ntext\"\"\"}";
        String expected = "{\"inlineThis\":\"multil${in}etext\"}";

        Method method = processor.getClass().getDeclaredMethod("performInlining", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(processor, json);
        assertEquals(expected, result);

        PipelineObject pipelineObject = new PipelineObject(result);
        assertTrue(pipelineObject.isValid());
    }

    @Test
    @WithoutJenkins
    public void inliningCommandsContainingQuotesShouldProduceValidJson() throws Exception {
        PipelineProcessor processor = getDefaultPipelineProcessor();
        String json = "{\"inlineThis\":\"\"\"multi\n\\\"line\\\"\nt'e'\\txt\"\"\"}";
        String expected = "{\"inlineThis\":\"multi\\\"line\\\"t'e'\\txt\"}";

        Method method = processor.getClass().getDeclaredMethod("performInlining", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(processor, json);
        assertEquals(expected, result);

        PipelineObject pipelineObject = new PipelineObject(result);
        assertTrue(pipelineObject.isValid());
    }

    private PipelineProcessor getDefaultPipelineProcessor() {
        BuildListener listener = Mockito.mock(BuildListener.class);
        Launcher launcher = Mockito.mock(Launcher.class);
        PipelineProcessor processor = new PipelineProcessor(getMockAbstractBuild(), launcher, listener);
        return processor;
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
