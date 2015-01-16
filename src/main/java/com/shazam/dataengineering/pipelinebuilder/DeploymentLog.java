package com.shazam.dataengineering.pipelinebuilder;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DTO object for deployment log
 */
public class DeploymentLog {
    private JSONObject log;
    private ParseException parseException;

    private static final String ROOT = "deployments";

    public DeploymentLog(String json) {
        try {
            JSONParser jsonParser = new JSONParser();
            log = (JSONObject) jsonParser.parse(json);
        } catch (ParseException e) {
            parseException = e;
        }
    }

    public DeploymentLog() {
        log = new JSONObject();
        log.put(ROOT, new JSONArray());
    }

    public void add(boolean status, Date date, List<String> messages) {
        add(new Deployment(status, date, messages));
    }

    public void add(Deployment deployment) {
        JSONArray deployments = (JSONArray) log.get(ROOT);
        deployments.add(deployment.toJSON());
    }

    public List<Deployment> getAll() {
        JSONArray deployments = (JSONArray) log.get(ROOT);
        ArrayList<Deployment> deploymentArrayList = new ArrayList<Deployment>();
        for (int i = 0; i < deployments.size(); i++) {
            deploymentArrayList.add(new Deployment((JSONObject) deployments.get(i)));
        }

        return deploymentArrayList;
    }

    public Deployment get(int index) {
        ArrayList<Deployment> all = (ArrayList<Deployment>) getAll();
        return all.get(index);
    }

    @Override
    public String toString() {
        return serialize();
    }

    public String serialize() {
        return log.toJSONString();
    }

    public boolean isParsed() {
        return parseException == null;
    }

    public ParseException getParseError() {
        return parseException;
    }

    public class Deployment {
        private boolean status;
        private Date date;
        private List<String> messages;

        public Deployment(boolean status, Date date, List<String> messages) {
            this.status = status;
            this.date = date;
            this.messages = messages;
        }

        public Deployment(JSONObject obj) {
            this.status = (Boolean) obj.get("status");
            this.date = new Date((Long) obj.get(date));
            JSONArray messageArray = (JSONArray) obj.get("messages");
            this.messages = new ArrayList<String>();
            for (int i = 0; i < messageArray.size(); i++) {
                this.messages.add((String) messageArray.get(i));
            }
        }

        public JSONObject toJSON() {
            JSONArray messageArray = new JSONArray();
            for (String message: messages) {
                messageArray.add(message);
            }

            JSONObject deployment = new JSONObject();
            deployment.put("status", status);
            deployment.put("date", date.getTime());
            deployment.put("messages", messageArray);

            return deployment;
        }
    }
}
