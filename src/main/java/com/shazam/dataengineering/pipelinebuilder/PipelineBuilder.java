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

import com.amazonaws.auth.BasicAWSCredentials;
import hudson.Extension;
import hudson.FilePath;
import hudson.Launcher;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.BuildListener;
import hudson.model.Descriptor;
import hudson.security.Permission;
import hudson.security.PermissionGroup;
import hudson.security.PermissionScope;
import hudson.tasks.BuildStepDescriptor;
import hudson.tasks.Builder;
import hudson.util.FormValidation;
import net.sf.json.JSONObject;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;
import org.kohsuke.stapler.StaplerRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PipelineBuilder extends Builder {
    public static final PermissionGroup PERMISSIONS = new PermissionGroup(
            PipelineBuilder.class, Messages._PipelineBuilder_PermissionsTitle());
    /**
     * Permission to trigger pipeline deploys.
     */
    public static final Permission DEPLOY_PERMISSION = new Permission(
            PERMISSIONS,
            "Deploy",
            Messages._PipelineBuilder_DeployPermission_Description(),
            null,
            PermissionScope.ITEM);
    private static ProductionEnvironment productionEnvironment = new ProductionEnvironment("Production");
    private static DevelopmentEnvironment developmentEnvironment = new DevelopmentEnvironment("Development");
    private Environment[] configParams;
    private String file;
    private String s3Prefix;

    // Fields in config.jelly must match the parameter names in the "DataBoundConstructor"
    @DataBoundConstructor
    public PipelineBuilder(String filePath, String s3Prefix, Environment[] environment) {
        this.configParams = environment;
        this.file = filePath;
        setS3Prefix(s3Prefix);
    }

    @Override
    public boolean perform(AbstractBuild build, Launcher launcher, BuildListener listener)
            throws IOException, InterruptedException {
        FilePath ws = build.getWorkspace();
        FilePath input = ws.child(file);

        PipelineProcessor processor = new PipelineProcessor(build, launcher, listener);
        if (configParams.length > 0) {
            processor.setEnvironments(configParams);
        } else {
            listener.getLogger().println("[WARN] No environments for AWS Data pipeline defined, skipping");
            return true;
        }
        processor.setS3Prefix(s3Prefix);

        boolean result = processor.process(input);
        if (result) {
            build.addAction(new DeploymentAction(
                    build,
                    processor.getS3Urls(),
                    new BasicAWSCredentials(
                            getDescriptor().getAccessId(),
                            getDescriptor().getSecretKey())));
        }

        return result;
    }

    /**
     * We'll use this from the <tt>config.jelly</tt>.
     */
    public Environment[] getConfigParams() {
        return configParams;
    }

    public String getFilePath() {
        return file;
    }

    public String getDisplayName() {
        return "AWS Data Pipeline";
    }

    public List<Environment> getEnvironmentList() {
        ArrayList<Environment> env = new ArrayList<Environment>();
        if (configParams != null) {
            for (Environment environment : configParams) {
                env.add(environment);
            }
        }
        return env;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public void setS3Prefix(String s3Prefix) {
        if (s3Prefix.endsWith("/")) {
            this.s3Prefix = s3Prefix;
        } else {
            this.s3Prefix = s3Prefix + "/";
        }
    }

    @Override
    public DescriptorImpl getDescriptor() {
        return (DescriptorImpl) super.getDescriptor();
    }

    /**
     * Descriptor for {@link PipelineBuilder}. Used as a singleton.
     * The class is marked as public so that it can be accessed from views.
     * <p/>
     */
    @Extension // This indicates to Jenkins that this is an implementation of an extension point.
    public static final class DescriptorImpl extends BuildStepDescriptor<Builder> {
        /**
         * To persist global configuration information,
         * simply store it in a field and call save().
         * <p/>
         * <p/>
         * If you don't want fields to be persisted, use <tt>transient</tt>.
         */
        public String accessId;
        public String secretKey;

        /**
         * In order to load the persisted global configuration, you have to
         * call load() in the constructor.
         */
        public DescriptorImpl() {
            load();
        }

        /**
         * Populates the "Add configuration" button with choices for environments
         *
         * @param project Instance
         * @return
         */
        public static List<Descriptor<Environment>> getEnvironmentDescriptions(AbstractProject<?, ?> project) {
            ArrayList<Descriptor<Environment>> descriptors = new ArrayList<Descriptor<Environment>>();
            descriptors.add(productionEnvironment.getDescriptor());
            descriptors.add(developmentEnvironment.getDescriptor());
            return descriptors;
        }

        public boolean isApplicable(Class<? extends AbstractProject> aClass) {
            // Indicates that this builder can be used with all kinds of project types
            return true;
        }

        public FormValidation doCheckS3Prefix(@QueryParameter String value) {
            if (!value.isEmpty() && !value.matches("^s3://([^/]+)/(.*)")) {
                return FormValidation.error("URL must be in the form \"s3://bucket/key/\"");
            } else {
                return FormValidation.ok();
            }
        }

        /**
         * This human readable name is used in the configuration screen.
         */
        public String getDisplayName() {
            return "AWS Data Pipeline";
        }

        @Override
        public boolean configure(StaplerRequest req, JSONObject formData) throws FormException {
            accessId = formData.getString("accessId");
            secretKey = formData.getString("secretKey");

            save();
            return super.configure(req, formData);
        }

        public String getAccessId() {
            return accessId;
        }

        public String getSecretKey() {
            return secretKey;
        }
    }
}
