package com.shazam.dataengineering.pipelinebuilder;

import hudson.FilePath;
import junit.framework.Assert;
import org.junit.Test;

import java.io.File;

public class PipelineObjectTest {
    @Test
    public void pipeline1shouldParseProperly() throws Exception {
        String json = new FilePath(new File("src/test/resources/pipeline1.json")).readToString();
        PipelineObject obj = new PipelineObject(json);

        Assert.assertTrue(obj.isValid());
    }

    @Test
    public void pipeline1shouldReParseIntoTheSameText() throws Exception {
        String json = new FilePath(new File("src/test/resources/pipeline1.json")).readToString();
        PipelineObject obj = new PipelineObject(json);
        String generatedJson = obj.getJson();
        PipelineObject reparsed = new PipelineObject(generatedJson);

        Assert.assertTrue(reparsed.isValid());
        Assert.assertEquals(generatedJson, reparsed.getJson());
    }

    @Test
    public void pipeline1shouldReplaceDate() throws Exception {
        String json = new FilePath(new File("src/test/resources/pipeline1.json")).readToString();
        String replacedJson = json.replace("2014-07-26T01:20:00", "2014-08-22T03:45:10");

        PipelineObject obj = new PipelineObject(json);
        PipelineObject validation = new PipelineObject(replacedJson);
        Assert.assertEquals("2014-07-26T01:20:00", obj.getScheduleDate());

        obj.setScheduleDate("2014-08-22T03:45:10");

        Assert.assertEquals("2014-08-22T03:45:10", obj.getScheduleDate());
        Assert.assertEquals(validation.getJson(), obj.getJson());
    }


}
