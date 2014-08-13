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
import net.sf.json.JSONObject;
import org.kohsuke.stapler.DataBoundConstructor;
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

    // Fields in config.jelly must match the parameter names in the "DataBoundConstructor"
    @DataBoundConstructor
    public PipelineBuilder(String filePath, Environment[] environment) {
        this.configParams = environment;
        this.file = filePath;
    }

    @Override
    public boolean perform(AbstractBuild build, Launcher launcher, BuildListener listener)
            throws IOException, InterruptedException {
        FilePath ws = build.getWorkspace();
        FilePath input = ws.child(file);

        PipelineProcessor processor = new PipelineProcessor(build, launcher, listener);
        processor.setEnvironments(configParams);

        boolean result = processor.process(input);
        if (result) {
            build.addAction(new DeploymentAction(build,
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
