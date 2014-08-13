package com.shazam.dataengineering.pipelinebuilder;

import hudson.FilePath;
import hudson.Launcher;
import hudson.model.AbstractBuild;
import hudson.model.BuildListener;

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
        // TODO
    }

    public boolean process(FilePath file) {
        if (checkExists(file)) {
            try {
                String text = file.readToString();
                int counter = 1;

                for (Environment env: environments) {
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
        String newJson = performSubstitutions(json, environment);
        List<String> warnings = warnForUnreplacedKeys(newJson);
        for (String warning: warnings) {
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
        Pattern pattern = Pattern.compile("(\\$\\{\\w?\\})");
        Matcher matcher = pattern.matcher(json);
        while (matcher.find()) {
            warnings.add(String.format("Unreplaced token found in pipeline object: %s", matcher.group()));
        }

        return warnings;
    }

    private String performSubstitutions(String json, Environment environment) {
        Map<String, String> substitutions = getSubstitutionMap(environment);
        json = substituteMapValues(json, substitutions);
        json = substituteScriptUrls(json);

        return json;
    }

    private String substituteMapValues(String json, Map<String, String> substitutions) {
        String pattern = "(\\$\\{%s\\})";
        for (String key: substitutions.keySet()) {
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

    private String substituteScriptUrls(String json) {
        Pattern pattern = Pattern.compile("(\\$\\{\\w?\\})");
        Matcher matcher = pattern.matcher(json);
        while (matcher.find()) {
            String potentialScript = matcher.group();
            if (archiveFile(potentialScript)) {
                // TODO: replace key by S3 URL path to the script
            }
        }

        // TODO: Replace files
//        build.getWorkspace().list()
//                build.getUpstreamBuilds()
//        build.getUpstreamBuilds().keySet()
//        .getLastBuild().getArtifacts()

        return json;
    }

    private boolean archiveFile(String filename) {
        return false;
    }

    private Map<String, String> getSubstitutionMap(Environment environment) {
        HashMap<String, String> substitutions = new HashMap<String, String>();
        String params = environment.getConfigParam();
        String[] lines = params.split("\\n");
        for (String kv: lines) {
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
