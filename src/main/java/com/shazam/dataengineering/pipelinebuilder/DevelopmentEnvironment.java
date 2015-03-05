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

import hudson.Extension;
import org.kohsuke.stapler.DataBoundConstructor;

public class DevelopmentEnvironment extends Environment {
    public DevelopmentEnvironment(String name) {
        super(name);
    }

    @DataBoundConstructor
    public DevelopmentEnvironment(String name, String configParam) {
        super(name, configParam);
    }

    @Extension
    public static final class DescriptorImpl extends EnvironmentDescriptor {
        @Override
        public String getType() {
            return "development";
        }

        @Override
        public String getDisplayName() {
            return "Development";
        }
    }
}
