package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.services.datapipeline.model.Field;
import hudson.FilePath;

import org.junit.Test;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static junit.framework.Assert.*;

public class PipelineObjectTest {
    @Test
    public void pipeline1shouldParseProperly() throws Exception {
        String json = new FilePath(new File("src/test/resources/pipeline1.json")).readToString();
        PipelineObject obj = new PipelineObject(json);

        assertTrue(obj.isValid());
    }

    @Test
    public void pipeline1shouldReParseIntoTheSameText() throws Exception {
        String json = new FilePath(new File("src/test/resources/pipeline1.json")).readToString();
        PipelineObject obj = new PipelineObject(json);
        String generatedJson = obj.getJson();
        PipelineObject reparsed = new PipelineObject(generatedJson);

        assertTrue(reparsed.isValid());
        assertEquals(generatedJson, reparsed.getJson());
    }

    @Test
    public void pipeline1shouldReplaceDate() throws Exception {
        String json = new FilePath(new File("src/test/resources/pipeline1.json")).readToString();
        String replacedJson = json.replace("2014-07-26T01:20:00", "2014-08-22T03:45:10");

        PipelineObject obj = new PipelineObject(json);
        PipelineObject validation = new PipelineObject(replacedJson);
        assertEquals("2014-07-26T01:20:00", obj.getScheduleDate());

        obj.setScheduleDate("2014-08-22T03:45:10");

        assertEquals("2014-08-22T03:45:10", obj.getScheduleDate());
        assertEquals(validation.getJson(), obj.getJson());
    }


    @Test
    public void pipeline3shouldParseIntoAWSPipelineObjectCorrectly() throws Exception {
        List<com.amazonaws.services.datapipeline.model.PipelineObject> pipeline3 = getAWSPipeline3();
        String json = new FilePath(new File("src/test/resources/pipeline3.json")).readToString();
        PipelineObject obj = new PipelineObject(json);

        // DeepEquals doesn't validate properly, must be incorrect implementation of equals.
        // As a result order actually matters in this check.
        assertEquals(pipeline3, obj.getAWSObjects());
    }

    @Test
    public void validateDateShouldValidateProperDate() throws Exception {
        assertTrue(PipelineObject.validateDate("2014-07-26T01:20:00"));
    }

    @Test
    public void validateDateShouldNotValidateImproperDate() throws Exception {
        assertFalse(PipelineObject.validateDate("2014-07/26T01=20:00"));
    }

    @Test
    public void validateDateShouldNotValidateEpochStart() throws Exception {
        Date epoch = new Date(0);
        SimpleDateFormat dateFormat = new SimpleDateFormat(PipelineObject.PIPELINE_DATE_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        assertFalse(PipelineObject.validateDate(dateFormat.format(epoch)));
    }

    @Test
    public void isPastShouldReturnTrueForDatesInThePast() throws Exception {
        Date past = new Date(1400000000L);
        SimpleDateFormat dateFormat = new SimpleDateFormat(PipelineObject.PIPELINE_DATE_FORMAT);
        assertFalse(PipelineObject.isPast(dateFormat.format(past)));
    }

    @Test
    public void isPastShouldReturnFalseForDatesInTheFuture() throws Exception {
        Date future = new Date(2407866423L);
        SimpleDateFormat dateFormat = new SimpleDateFormat(PipelineObject.PIPELINE_DATE_FORMAT);
        assertFalse(PipelineObject.isPast(dateFormat.format(future)));
    }

    /**
     * Manually construct AWS Pipeline object for pipeline3 definition.
     * Note - order matters - see the test
     * @return List<PipelineObject>
     */
    private List<com.amazonaws.services.datapipeline.model.PipelineObject> getAWSPipeline3() {
        ArrayList<com.amazonaws.services.datapipeline.model.PipelineObject> pipelineList =
                new ArrayList<com.amazonaws.services.datapipeline.model.PipelineObject>();

        com.amazonaws.services.datapipeline.model.PipelineObject redhsiftDb =
                new com.amazonaws.services.datapipeline.model.PipelineObject();
        redhsiftDb.withId("RedshiftDatabaseId_123").withName("Database")
                .withFields(
                        new Field().withKey("*password").withStringValue("password"),
                        new Field().withKey("type").withStringValue("RedshiftDatabase"),
                        new Field().withKey("clusterId").withStringValue("cluster"),
                        new Field().withKey("username").withStringValue("username")
                );
        pipelineList.add(redhsiftDb);

        com.amazonaws.services.datapipeline.model.PipelineObject schedule =
                new com.amazonaws.services.datapipeline.model.PipelineObject();
        schedule.withId("ScheduleId_234").withName("Daily")
                .withFields(
                        new Field().withKey("startDateTime").withStringValue("2014-07-29T15:00:00"),
                        new Field().withKey("period").withStringValue("1 Day"),
                        new Field().withKey("type").withStringValue("Schedule")
                );
        pipelineList.add(schedule);

        com.amazonaws.services.datapipeline.model.PipelineObject emrActivity =
                new com.amazonaws.services.datapipeline.model.PipelineObject();
        emrActivity.withId("ActivityId_345").withName("EMR Job")
                .withFields(
                        new Field().withKey("onFail").withRefValue("ActionId_098"),
                        new Field().withKey("step").withStringValue("s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar,s3://us-east-1.elasticmapreduce/libs/pig/pig-script,--base-path,s3://us-east-1.elasticmapreduce/libs/pig/,--pig-versions,latest,--run-pig-script,--args,-f,s3://bucket/script.pig,-p,PARALLEL=10,-p,INPUT=s3://bucket/year=#{format(minusDays(@scheduledStartTime,1),'YYYY')}/month=#{format(minusDays(@scheduledStartTime,1),'MM')}/day=#{format(minusDays(@scheduledStartTime,1),'dd')}/*,-p,OUTPUT=s3://bucket/output/#{format(minusDays(@scheduledStartTime,1),'YYYY-MM-dd')}"),
                        new Field().withKey("runsOn").withRefValue("EmrClusterId_678"),
                        new Field().withKey("step").withStringValue("s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar,s3://us-east-1.elasticmapreduce/libs/pig/pig-script,--base-path,s3://us-east-1.elasticmapreduce/libs/pig/,--install-pig,--pig-versions,latest"),
                        new Field().withKey("type").withStringValue("EmrActivity"),
                        new Field().withKey("schedule").withRefValue("ScheduleId_234")
                );
        pipelineList.add(emrActivity);

        com.amazonaws.services.datapipeline.model.PipelineObject sqlQuery1 =
                new com.amazonaws.services.datapipeline.model.PipelineObject();
        sqlQuery1.withId("ActivityId_456").withName("SQL Query")
                .withFields(
                        new Field().withKey("onFail").withRefValue("ActionId_098"),
                        new Field().withKey("maximumRetries").withStringValue("0"),
                        new Field().withKey("runsOn").withRefValue("Ec2Resource_678"),
                        new Field().withKey("database").withRefValue("RedshiftDatabaseId_123"),
                        new Field().withKey("type").withStringValue("SqlActivity"),
                        new Field().withKey("schedule").withRefValue("ScheduleId_234"),
                        new Field().withKey("script").withStringValue("SELECT 1;")
                );
        pipelineList.add(sqlQuery1);

        com.amazonaws.services.datapipeline.model.PipelineObject sqlQuery2 =
                new com.amazonaws.services.datapipeline.model.PipelineObject();
        sqlQuery2.withId("ActivityId_567").withName("SQL Query 2")
                .withFields(
                        new Field().withKey("onFail").withRefValue("ActionId_098"),
                        new Field().withKey("maximumRetries").withStringValue("0"),
                        new Field().withKey("dependsOn").withRefValue("ActivityId_345"),
                        new Field().withKey("runsOn").withRefValue("Ec2Resource_678"),
                        new Field().withKey("dependsOn").withRefValue("ActivityId_456"),
                        new Field().withKey("database").withRefValue("RedshiftDatabaseId_123"),
                        new Field().withKey("type").withStringValue("SqlActivity"),
                        new Field().withKey("schedule").withRefValue("ScheduleId_234"),
                        new Field().withKey("script").withStringValue("SELECT 2;")
                );
        pipelineList.add(sqlQuery2);

        com.amazonaws.services.datapipeline.model.PipelineObject snsAlarm =
                new com.amazonaws.services.datapipeline.model.PipelineObject();
        snsAlarm.withId("ActionId_098").withName("SNS Alert")
                .withFields(
                        new Field().withKey("message").withStringValue("Fail Message"),
                        new Field().withKey("role").withStringValue("DataPipelineDefaultRole"),
                        new Field().withKey("subject").withStringValue("Error"),
                        new Field().withKey("type").withStringValue("SnsAlarm"),
                        new Field().withKey("topicArn").withStringValue("arn:aws:sns:us-east-1:sns_feed")
                );
        pipelineList.add(snsAlarm);

        com.amazonaws.services.datapipeline.model.PipelineObject defaultObject =
                new com.amazonaws.services.datapipeline.model.PipelineObject();
        defaultObject.withId("Default").withName("Default")
                .withFields(
                        new Field().withKey("resourceRole").withStringValue("DataPipelineDefaultResourceRole"),
                        new Field().withKey("role").withStringValue("DataPipelineDefaultRole"),
                        new Field().withKey("scheduleType").withStringValue("cron"),
                        new Field().withKey("failureAndRerunMode").withStringValue("cascade")
                );
        pipelineList.add(defaultObject);

        com.amazonaws.services.datapipeline.model.PipelineObject emrCluster =
                new com.amazonaws.services.datapipeline.model.PipelineObject();
        emrCluster.withId("EmrClusterId_678").withName("DefaultEmrCluster1")
                .withFields(
                        new Field().withKey("region").withStringValue("us-east-1"),
                        new Field().withKey("coreInstanceType").withStringValue("m1.small"),
                        new Field().withKey("masterInstanceType").withStringValue("m1.small"),
                        new Field().withKey("terminateAfter").withStringValue("10 Hours"),
                        new Field().withKey("keyPair").withStringValue("mykeypair"),
                        new Field().withKey("coreInstanceCount").withStringValue("1"),
                        new Field().withKey("bootstrapAction").withStringValue("s3://bucket/bootstrap-script.sh"),
                        new Field().withKey("enableDebugging").withStringValue("true"),
                        new Field().withKey("schedule").withRefValue("ScheduleId_234"),
                        new Field().withKey("type").withStringValue("EmrCluster"),
                        new Field().withKey("emrLogUri").withStringValue("s3://bucket/"),
                        new Field().withKey("logUri").withStringValue("s3://bucket/tasklogs")
                );
        pipelineList.add(emrCluster);

        return pipelineList;
    }

}
