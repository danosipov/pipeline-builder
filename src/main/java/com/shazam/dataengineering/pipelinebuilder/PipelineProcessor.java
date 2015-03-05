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

import hudson.FilePath;
import hudson.Launcher;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.BuildListener;
import hudson.model.Run;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PipelineProcessor {
    public static final String FILE_NAME_FORMAT = "%s%d-%s-%d.json";

    private AbstractBuild build;
    private BuildListener listener;
    private Launcher launcher;
    private ArrayList<Environment> environments;
    private String name;
    private int buildNumber;
    private String s3Url;
    private HashMap<S3Environment, String> s3ScriptToUrl = new HashMap<S3Environment, String>();

    public PipelineProcessor(AbstractBuild build, Launcher launcher, BuildListener listener) {
        this.listener = listener;
        this.launcher = launcher;
        this.build = build;

        this.name = build.getProject().getName().replace(" ", "-");
        this.buildNumber = build.getNumber();
    }

    public void setEnvironments(Environment[] environmentArray) {
        environments = new ArrayList<Environment>();
        environments.addAll(Arrays.asList(environmentArray));
    }

    public void setS3Prefix(String s3Url) {
        this.s3Url = s3Url;
    }

    public Map<S3Environment, String> getS3Urls() {
        return s3ScriptToUrl;
    }

    public boolean process(FilePath file) {
        if (checkExists(file)) {
            try {
                String text = file.readToString();
                int counter = 1;

                for (Environment env : environments) {
                    String fileName = getFileName(env, counter);
                    counter += 1;
                    storeProcessedFile(fileName, text, env);
                    writeDOT(fileName);
                    // TODO: attempt to convert to png
                    // Using CLI: dot -Tpng input.dot > output.png
                }
                return true;
            } catch (IOException e) {
                listener.error("Failed to read the pipeline object");
                return false;
            }
        } else {
            return false;
        }
    }

    private void writeDOT(String filename) throws IOException {
        FilePath pipelinePath = new FilePath(new FilePath(build.getArtifactsDir()), filename);
        PipelineObject pipelineObject = new PipelineObject(pipelinePath.readToString());
        FileWriter dotWriter = new FileWriter(new File(build.getArtifactsDir(), filename.replace(".json", ".dot")));

        pipelineObject.writeDOT(dotWriter);
    }

    private String getFileName(Environment environment, int counter) {
        String prefix;
        if (environment instanceof DevelopmentEnvironment) {
            prefix = "d";
        } else if (environment instanceof ProductionEnvironment) {
            prefix = "p";
        } else {
            prefix = "u";
        }

        return String.format(FILE_NAME_FORMAT, prefix, counter, name, buildNumber);
    }

    private boolean storeProcessedFile(String fileName, String json, Environment environment) {
        String singleLineJson = performInlining(json);
        String newJson = performSubstitutions(singleLineJson, fileName, environment);
        List<String> warnings = warnForUnreplacedKeys(newJson);
        for (String warning : warnings) {
            listener.getLogger().println("[WARN] " + warning);
        }

        // Validate created pipeline
        PipelineObject pipelineObject = new PipelineObject(json);
        if (!pipelineObject.isValid()) {
            listener.error("Resulting JSON file is invalid pipeline object");
            return false;
        }

        FilePath newPath = new FilePath(new FilePath(build.getArtifactsDir()), fileName);
        try {
            newPath.copyFrom(new ByteArrayInputStream((newJson.getBytes(StandardCharsets.UTF_8))));
            return true;
        } catch (IOException e) {
            listener.getLogger().println(e);
            return false;
        } catch (InterruptedException e) {
            listener.getLogger().println(e);
            return false;
        }
    }

    private List<String> warnForUnreplacedKeys(String json) {
        ArrayList<String> warnings = new ArrayList<String>();
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(json);
        while (matcher.find()) {
            warnings.add(String.format("Unreplaced token found in pipeline object: %s", matcher.group()));
        }

        return warnings;
    }

    /**
     * Convert multiline strings into single line string
     * Result should be a valid JSON.
     * <p/>
     * Example:
     * """ test
     * string""" => " test    string"
     * NOTE: Spaces get preserved
     *
     * @param json
     * @return
     */
    private String performInlining(String json) {
        Pattern pattern = Pattern.compile("\"\"\"([^\"\\\\]*(\\\\.[^\"\\\\]*)*)\"\"\"", Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(json);
        StringBuffer jsonBuffer = new StringBuffer();

        while (matcher.find()) {
            String longText = matcher.group();
            String sql = longText.substring(2, longText.length() - 2);
            String replacement = sql
                    .replace("\n", "")
                    .replace("\\", "\\\\")
                    .replace("$", "\\$");
            matcher.appendReplacement(jsonBuffer, replacement);
        }
        matcher.appendTail(jsonBuffer);

        return jsonBuffer.toString();
    }

    /**
     * Substitute keys in passed in json by corresponding values.
     * First pass substitutes environment variables as defined in the build configuration
     * Second pass looks for scripts to be replaced.
     *
     * @param json
     * @param pipelineName
     * @param environment
     * @return
     */
    private String performSubstitutions(String json, String pipelineName, Environment environment) {
        Map<String, String> substitutions = getSubstitutionMap(environment);
        json = substituteMapValues(json, substitutions);

        // If s3Url is defined, process any unreplaced tokens as scripts
        if (s3Url != null && !s3Url.isEmpty()) {
            json = substituteScriptUrls(json, pipelineName);
        }

        return json;
    }

    private String substituteMapValues(String json, Map<String, String> substitutions) {
        String pattern = "(\\$\\{%s\\})";
        for (String key : substitutions.keySet()) {
            String replacement = Matcher.quoteReplacement(substitutions.get(key));
            try {
                json = json.replaceAll(
                        String.format(pattern, Pattern.quote(key)),
                        replacement);
            } catch (IllegalArgumentException e) {
                listener.error("Failed to replace %s by %s.", key, replacement);
            }
        }

        return json;
    }

    /**
     * Look through json for unreplaced tokens, and see if we can match
     * them to any files in the workspace. If we can, save the file in the artifacts.
     * During the deployment, the files would be uploaded to a special S3 bucket for
     * this job. The token is preemptively replaced by this URL.
     * <p/>
     * This method assumes s3Url is set properly.
     *
     * @param json
     * @param pipelineName
     * @return
     */
    private String substituteScriptUrls(String json, String pipelineName) {
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(json);
        HashMap<String, String> substitutions = new HashMap<String, String>();

        while (matcher.find()) {
            String token = matcher.group();
            String potentialScript = token.substring(2, token.length() - 1);
            try {
                if (archiveFile(potentialScript)) {
                    String scriptUrl = s3Url
                            + pipelineName.substring(0, pipelineName.lastIndexOf(".json"))
                            + "/" + potentialScript;
                    s3ScriptToUrl.put(new S3Environment(pipelineName, potentialScript), scriptUrl);
                    substitutions.put(potentialScript, scriptUrl);
                }
            } catch (Exception e) {
                listener.error("Error in substituting script URL: " + e.getMessage());
            }
        }

        return substituteMapValues(json, substitutions);
    }

    /**
     * Iterates over current workspace and upstream project artifacts to find the defined file name
     * If found, archive it as an artifact to make available to the deployment action.
     *
     * @param filename
     * @return
     */
    private boolean archiveFile(String filename) throws IOException, InterruptedException {
        FilePath newPath = new FilePath(new FilePath(build.getArtifactsDir()),
                "scripts/" + filename);
        if (newPath.exists()) {
            // File already copied (perhaps for a different environment
            return true;
        }

        // First look recursively in current workspace
        if (scanDirectory(build.getWorkspace(), filename)) {
            return true;
        }

        // Second look in upstream projects
        Set<AbstractProject> upstreamProjects = build.getUpstreamBuilds().keySet();
        for (AbstractProject project : upstreamProjects) {
            List<Run.Artifact> artifacts = project.getLastBuild().getArtifacts();
            for (Run.Artifact artifact : artifacts) {
                if (artifact.getFileName().equals(filename)) {
                    newPath.copyFrom(new FilePath(artifact.getFile()));
                    return true;
                }
            }

        }

        return false;
    }

    private boolean scanDirectory(FilePath directory, String filename) throws IOException, InterruptedException {
        for (FilePath path : directory.list()) {
            if (path.isDirectory() && scanDirectory(path, filename)) {
                return true;
            } else if (path.getName().equals(filename)) {
                listener.getLogger().println("[INFO] Found an artifact at " + path.getName());
                FilePath newPath = new FilePath(new FilePath(build.getArtifactsDir()),
                        "scripts/" + filename);
                newPath.copyFrom(path.read());
                return true;
            }
        }

        return false;
    }

    private Map<String, String> getSubstitutionMap(Environment environment) {
        HashMap<String, String> substitutions = new HashMap<String, String>();
        String params = environment.getConfigParam();
        String[] lines = params.split("\\n");
        for (String kv : lines) {
            String[] keyAndValue = kv.split(":", 2);
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