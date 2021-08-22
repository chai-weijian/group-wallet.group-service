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

import com.chaiweijian.groupwallet.groupservice.interfaces.SimpleValidator;
import com.chaiweijian.groupwallet.groupservice.v1.Group;
import com.google.rpc.BadRequest;
import lombok.Data;

import java.util.List;

@Data
public class SimpleGroupValidator implements SimpleValidator {
    private final Group group;
    private final BadRequest badRequest;
    private final boolean failed;

    private SimpleGroupValidator(Group group, BadRequest badRequest, boolean failed) {
        this.group = group;
        this.badRequest = badRequest;
        this.failed = failed;
    }

    public static SimpleGroupValidator validate(Group group) {
        var builder = BadRequest.newBuilder();

        validateDisplayName(builder, group);
        validateCurrencyCode(builder, group);

        return new SimpleGroupValidator(group, builder.build(), builder.getFieldViolationsCount() > 0);
    }

    private static void validateDisplayName(BadRequest.Builder badRequestBuilder, Group group) {
        var displayName = group.getDisplayName();

        if (displayName.length() == 0) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("group.displayName").setDescription("Group display name must not be empty."));
        } else if (displayName.length() < 4) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("group.displayName").setDescription("Group display name must has minimum 4 characters."));
        } else if (displayName.length() > 120) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("group.displayName").setDescription("Group display name must has maximum 120 characters."));
        }
    }

    private static void validateCurrencyCode(BadRequest.Builder badRequestBuilder, Group group) {
        final var SUPPORTED_CURRENCY = List.of("SGD", "MYR");

        var currencyCode = group.getCurrencyCode();

        if (!SUPPORTED_CURRENCY.contains(currencyCode)) {
            badRequestBuilder.addFieldViolations(BadRequest.FieldViolation.newBuilder().setField("group.currencyCode").setDescription(String.format("%s is not supported. Supported currency are %s", currencyCode, String.join(", ", SUPPORTED_CURRENCY))));
        }
    }
}
