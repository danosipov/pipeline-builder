package com.shazam.dataengineering.pipelinebuilder;

import com.amazonaws.services.datapipeline.model.Field;
import org.jgrapht.ext.*;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.*;

public class PipelineObject {
    public static final String PIPELINE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    private JSONObject pipeline;
    private ParseException parseException;

    public PipelineObject(String json) {
        try {
            JSONParser jsonParser = new JSONParser();
            pipeline = (JSONObject) jsonParser.parse(json);
        } catch (ParseException e) {
            parseException = e;
        }
    }

    public boolean isValid() {
        return pipeline != null;
    }

    public Exception getError() {
        return parseException;
    }

    public void setScheduleDate(String date) {
        if (isValid() && validateDate(date)) {
            JSONArray objectArray = (JSONArray) pipeline.get("objects");
            for (Object object : objectArray) {
                Object type = ((JSONObject) object).get("type");
                // TODO: Handle multiple Schedule objects
                // TODO: Handle runOnce objects
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

    public static Date getDate(String date) throws java.text.ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat(PIPELINE_DATE_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.parse(date);
    }

    public static boolean isPast(String date) {
        try {
            return !getDate(date).after(new Date());
        } catch (java.text.ParseException e) {
            return false; // Senseless response. Assume user has already validated the date
        }
    }

    public static boolean validateDate(String date) {
        try {
            return getDate(date).after(new Date(1)); // crude check that we don't have epoch start
        } catch (java.text.ParseException e) {
            return false;
        }
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
                LinkedHashSet<Field> fields = new LinkedHashSet<Field>();
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

    public void writeDOT(Writer writer) {
        new GraphWriter().writeDOT(writer, this);
    }

    private HashSet<Field> parseFields(LinkedHashSet<Field> accumulator, Object json, String key) {
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
