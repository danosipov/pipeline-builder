package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.services.datapipeline.model.Field;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PipelineObject {
    private JSONObject pipeline;

    public PipelineObject(String json) {
        try {
            JSONParser jsonParser = new JSONParser();
            pipeline = (JSONObject) jsonParser.parse(json);
        } catch (ParseException e) {
            // TODO: Store error, perhaps it would be helpful
            //e.printStackTrace();
        }
    }

    public boolean isValid() {
        return pipeline != null;
    }

    public void setScheduleDate(String date) {
        // TODO: Validate the date, warn if in the past
        if (isValid()) {
            JSONArray objectArray = (JSONArray) pipeline.get("objects");
            for (Object object : objectArray) {
                Object type = ((JSONObject) object).get("type");
                if (type != null && type.toString().equals("Schedule")) {
                    ((JSONObject) object).put("startDateTime", date);
                }
            }
        }
    }

    public String getScheduleDate() {
        if (isValid()) {
            JSONArray objectArray = (JSONArray) pipeline.get("objects");
            for (Object object : objectArray) {
                Object type = ((JSONObject) object).get("type");
                if (type != null && type.toString().equals("Schedule")) {
                    return (String) ((JSONObject) object).get("startDateTime");
                }
            }
        }

        return "";
    }

    public String getJson() {
        return pipeline.toJSONString();
    }

    /**
     * Parse the JSON file into AWS Pipeline model.
     * Warning: Lots of ugly casts here.
     * TODO: See if can be parsed safely in GSON or Jackson.
     *
     * @return AWS PipelineObject
     */
    public List<com.amazonaws.services.datapipeline.model.PipelineObject> getAWSObjects() {
        ArrayList<com.amazonaws.services.datapipeline.model.PipelineObject> list =
                new ArrayList<com.amazonaws.services.datapipeline.model.PipelineObject>();
        if (isValid()) {
            JSONArray objectArray = (JSONArray) pipeline.get("objects");
            for (Object object : objectArray) {
                com.amazonaws.services.datapipeline.model.PipelineObject pipelineObject
                        = new com.amazonaws.services.datapipeline.model.PipelineObject();
                HashSet<Field> fields = new HashSet<Field>();
                JSONObject jsonObject = (JSONObject) object;

                for (String key: (Set<String>) jsonObject.keySet()) {
                    if (key.equals("id")) {
                        pipelineObject.setId((String) jsonObject.get(key));
                    } else if (key.equals("name")) {
                        pipelineObject.setName((String) jsonObject.get(key));
                    } else {
                        parseFields(fields, jsonObject.get(key), key);
                    }
                }

                pipelineObject.setFields(fields);
                list.add(pipelineObject);
            }
        }

        return list;
    }

    private HashSet<Field> parseFields(HashSet<Field> accumulator, Object json, String key) {
        if (json instanceof String) {
            accumulator.add(new Field().withKey(key).withStringValue((String) json));
        } else if (json instanceof JSONArray) {
            JSONArray fieldArray = (JSONArray) json;
            for (Object field: fieldArray) {
                parseFields(accumulator, field, key);
            }
        } else if (json instanceof JSONObject) {
            String refValue = (String) ((JSONObject) json).get("ref");
            accumulator.add(new Field().withKey(key).withRefValue(refValue));
        }

        return accumulator;
    }
}
