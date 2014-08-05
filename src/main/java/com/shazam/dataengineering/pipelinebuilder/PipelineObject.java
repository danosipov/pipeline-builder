package com.shazam.dataengineering.pipelinebuilder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class PipelineObject {
    private JSONObject pipeline;

    public PipelineObject(String json) {
//        ObjectMapper mapper = new ObjectMapper();
////        mapper.setPropertyNamingStrategy(
////                new CamelCaseNamingStrategy());
//        try {
//            pipeline = mapper.readValue(json, PipelineRoot.class);
//        } catch (Exception e) {
//            // Ignore
//
//            // TODO: Store error, perhaps it would be helpful
//            e.printStackTrace();
//        }

        try {
            JSONParser jsonParser = new JSONParser();
            pipeline = (JSONObject) jsonParser.parse(json);
        } catch (ParseException e) {
            // TODO: Store error, perhaps it would be helpful
            e.printStackTrace();
        }
    }

    public boolean isValid() {
        return pipeline != null;
    }

    public void setScheduleDate(String date) {
        // TODO: Validate the date, warn if in the past
        JSONArray objectArray = (JSONArray) pipeline.get("objects");
        for (Object object : objectArray) {
            Object type = ((JSONObject) object).get("type");
            if (type != null && type.toString().equals("Schedule")) {
                ((JSONObject) object).put("startDateTime", date);
            }
        }
    }

    public String getScheduleDate() {
        JSONArray objectArray = (JSONArray) pipeline.get("objects");
        for (Object object : objectArray) {
            Object type = ((JSONObject) object).get("type");
            if (type != null && type.toString().equals("Schedule")) {
                return (String) ((JSONObject) object).get("startDateTime");
            }
        }

        return "";
    }

    public String getJson() {
//        ObjectMapper mapper = new ObjectMapper();
//        try {
//            return mapper.writerWithType(PipelineRoot.class).writeValueAsString(pipeline);
//        } catch (JsonProcessingException e) {
//            // TODO: Log exception
//            return "";
//        }
        return pipeline.toJSONString();
    }
}
