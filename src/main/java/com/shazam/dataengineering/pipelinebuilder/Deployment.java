package com.shazam.dataengineering.pipelinebuilder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
* Deployment Data Transfer Object. Converts to and from a JSON entry.
* @see com.shazam.dataengineering.pipelinebuilder.DeploymentLog
*/
public class Deployment {
    private String username;
    private boolean status;
    private Date date;
    private String pipelineId;
    private List<String> messages;
    private static final DateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    public Deployment(String username, boolean status, String pipelineId, Date date, List<String> messages) {
        this.username = username;
        this.status = status;
        this.date = date;
        this.messages = messages;
        this.pipelineId = pipelineId;
    }

    public Deployment(JSONObject obj) {
        this.username = (String) obj.get("username");
        this.status = (Boolean) obj.get("status");
        this.pipelineId = (String) obj.get("pipelineId");
        this.date = new Date((Long) obj.get("date"));
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
        deployment.put("username", username);
        deployment.put("status", status);
        deployment.put("pipelineId", pipelineId);
        deployment.put("date", date.getTime());
        deployment.put("messages", messageArray);

        return deployment;
    }

    public boolean isSuccess() {
        return status;
    }

    public boolean getStatus() {
        return status;
    }

    public String getUsername() {
        return username;
    }

    public Date getDate() {
        return date;
    }

    public String getISODate() {
        return isoFormat.format(date);
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public List<String> getMessages() {
        return messages;
    }
}
