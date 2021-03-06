/*
 *   Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */

package io.siddhi.distribution.editor.core;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.distribution.common.common.SiddhiAppRuntimeService;
import io.siddhi.distribution.editor.core.internal.DebugRuntime;
import io.siddhi.distribution.editor.core.internal.EditorDataHolder;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains the implementations of the apis related to SiddhiAppRuntimes.
 */
public class EditorSiddhiAppRuntimeService implements SiddhiAppRuntimeService {

    @Override
    public Map<String, SiddhiAppRuntime> getActiveSiddhiAppRuntimes() {

        Map<String, DebugRuntime> siddhiApps = EditorDataHolder.getSiddhiAppMap();
        Map<String, SiddhiAppRuntime> siddhiAppRuntimes = new HashMap<>();
        for (Map.Entry<String, DebugRuntime> entry : siddhiApps.entrySet()) {
            if (entry.getValue() != null && (entry.getValue().getMode() == DebugRuntime.Mode.RUN ||
                    entry.getValue().getMode() == DebugRuntime.Mode.DEBUG)) {
                siddhiAppRuntimes.put(entry.getKey(), entry.getValue().getSiddhiAppRuntime());
            }
        }
        return siddhiAppRuntimes;
    }

    @Override
    public void enableSiddhiAppStatistics(Level enabledStatsLevel) {
        //ignore the editor runtime statistics
    }
}
