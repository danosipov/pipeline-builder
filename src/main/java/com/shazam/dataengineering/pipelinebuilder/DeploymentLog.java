/*
 * Copyright 2015 Shazam Entertainment Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License
 */
package com.shazam.dataengineering.pipelinebuilder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
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

    public void add(String username, boolean status, String pipelineId, Date date, List<String> messages) {
        add(new Deployment(username, status, pipelineId, date, messages));
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

}
