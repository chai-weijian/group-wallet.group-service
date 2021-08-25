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

package com.chaiweijian.groupwallet.groupservice.member.remove;

import com.chaiweijian.groupwallet.groupservice.util.GroupAggregateUtil;
import com.chaiweijian.groupwallet.groupservice.util.OkStatusUtil;
import com.chaiweijian.groupwallet.groupservice.util.RequestAndExistingGroup;
import com.chaiweijian.groupwallet.groupservice.util.StreamValidationResult;
import com.chaiweijian.groupwallet.groupservice.util.ValidationResult;
import com.google.rpc.Code;
import com.google.rpc.Status;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import com.chaiweijian.groupwallet.groupservice.v1.RemoveMemberRequest;
import com.chaiweijian.groupwallet.groupservice.v1.Group;

import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class RemoveMemberRequestProcessor {
    @Bean
    public Function<KStream<String, RemoveMemberRequest>, Function<GlobalKTable<String, Group>, KStream<String, Status>>> removeMember() {
        return removeMemberRequest -> groupAggregateStore -> {
            var requestAndGroup = removeMemberRequest
                    .leftJoin(groupAggregateStore,
                            (leftKey, leftValue) -> leftKey,
                            RequestAndExistingGroup::new);

            var groupExistsValidation = validateGroupExists(requestAndGroup);

            var memberExistsValidation = validateMemberExists(groupExistsValidation.getPassedStream());

            memberExistsValidation.getPassedStream()
                    .map(((key, value) -> KeyValue.pair(value.getRequest().getGroup(), value.getRequest().getMember())))
                    .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
                    .to("groupwallet.groupservice.MemberRemoved-events");

            var group = memberExistsValidation
                    .getPassedStream()
                    .mapValues(value -> value.getCurrentGroup().toBuilder()
                            .clearMembers()
                            .addAllMembers(value.getCurrentGroup().getMembersList().stream().filter(member -> !member.equals(value.getRequest().getMember())).collect(Collectors.toList()))
                            .setAggregateVersion(value.getCurrentGroup().getAggregateVersion() + 1)
                            .setEtag(GroupAggregateUtil.calculateEtag(value.getCurrentGroup().getName(), value.getCurrentGroup().getAggregateVersion() + 1))
                            .build());

            var successStatus = group.mapValues(value -> OkStatusUtil.packStatus(value, "Member removed."));

            return groupExistsValidation.getStatusStream()
                    .merge(memberExistsValidation.getStatusStream())
                    .merge(successStatus);
        };
    }

    private static StreamValidationResult<String, RequestAndExistingGroup<RemoveMemberRequest>> validateGroupExists(KStream<String, RequestAndExistingGroup<RemoveMemberRequest>> input) {
        var validation = input.mapValues(value -> new ValidationResult<>(value).setFail(value.getCurrentGroup() == null));

        var failed = validation
                .filter((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.NOT_FOUND_VALUE)
                        .setMessage(String.format("Group with name %s does not exists.", value.getRequest().getGroup()))
                        .build());

        var passed = validation
                .filter(((key, value) -> value.isPassed()))
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private static StreamValidationResult<String, RequestAndExistingGroup<RemoveMemberRequest>> validateMemberExists(KStream<String, RequestAndExistingGroup<RemoveMemberRequest>> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(value.getCurrentGroup().getMembersList().contains(value.getRequest().getMember())));

        var failed = validation
                .filter((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.NOT_FOUND_VALUE)
                        .setMessage(String.format("Member with name %s does not exists.", value.getRequest().getMember()))
                        .build());

        var passed = validation
                .filter(((key, value) -> value.isPassed()))
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }
}
