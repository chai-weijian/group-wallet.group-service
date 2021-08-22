// Copyright 2021 Chai Wei Jian
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.chaiweijian.groupwallet.groupservice.util;

import com.chaiweijian.groupwallet.groupservice.v1.Group;
import org.apache.kafka.streams.kstream.KStream;

public class GroupStreamValidatorUtil {
    public static StreamValidationResult<String, Group> validateSimple(KStream<String, Group> input) {
        var simpleValidation = input
                .mapValues(SimpleGroupValidator::validate);

        var failed = simpleValidation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(SimpleGroupValidator::getGroup);

        var status = simpleValidation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(BadRequestUtil::packStatus);

        var passed = simpleValidation
                .filterNot(((key, value) -> value.isFailed()))
                .mapValues(SimpleGroupValidator::getGroup);

        return new StreamValidationResult<>(passed, failed, status);
    }

}
