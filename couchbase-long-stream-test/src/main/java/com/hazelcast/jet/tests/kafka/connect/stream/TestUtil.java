/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.tests.kafka.connect.stream;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

public final class TestUtil {

    private TestUtil() {
    }

    static String getDocId(SourceRecord rec) {
        Struct value = (Struct) rec.value();
        return value.getString("docId");
    }

    static String convertToString(SourceRecord rec) {
        return Values.convertToString(rec.valueSchema(), rec.value());
    }

}
