package com.shazam.dataengineering.pipelinebuilder;

import hudson.FilePath;
import hudson.model.BuildListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PipelineProcessor {
    private BuildListener listener;
    private ArrayList<Environment> environments;

    public PipelineProcessor(BuildListener listener) {
        this.listener = listener;
    }

    public void setEnvironments(Environment[] environmentArray) {
        environments = new ArrayList<Environment>();
        environments.addAll(Arrays.asList(environmentArray));
    }

    public boolean process(FilePath file) {
        if (checkExists(file)) {
            // TEMP:
            return true;
        } else {
            return false;
        }
    }

    private String performSubstitutions(String json, Environment environment) {
        Map<String, String> substitutions = getSubstitutionMap(environment);
        String pattern = "(${%s})";
        for (String key: substitutions.keySet()) {
            json = json.replaceAll(String.format(pattern, key), substitutions.get(key));
        }

        return json;
    }

    private Map<String, String> getSubstitutionMap(Environment environment) {
        HashMap<String, String> substitutions = new HashMap<String, String>();
        String params = environment.getConfigParam();
        String[] lines = params.split("\\n");
        for (String kv: lines) {
            String[] keyAndValue = kv.split(":");
            if (keyAndValue.length == 2) {
                String key = keyAndValue[0].trim();
                String value = keyAndValue[1].trim();
                substitutions.put(key, value);
            }
        }

        return substitutions;
    }

    private boolean checkExists(FilePath input) {
        try {
            if (!input.exists()) {
                listener.error("Pipeline file does not exist in the workspace directory. Check the path in the build configuration");
                listener.error("Path checked %s", input.absolutize().toURI());
                return false;
            }
        } catch (IOException io) {
            listener.fatalError("Error accessing the pipeline object");
            listener.getLogger().println(io);
            return false;
        } catch (InterruptedException ie) {
            listener.fatalError("Error accessing the pipeline object");
            listener.getLogger().println(ie);
            return false;
        }
        return true;
    }
}
