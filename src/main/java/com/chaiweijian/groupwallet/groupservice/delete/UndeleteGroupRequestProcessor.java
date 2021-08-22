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

package com.chaiweijian.groupwallet.groupservice.delete;

import com.chaiweijian.groupwallet.groupservice.util.GroupAggregateUtil;
import com.chaiweijian.groupwallet.groupservice.util.OkStatusUtil;
import com.chaiweijian.groupwallet.groupservice.util.RequestAndExistingGroup;
import com.chaiweijian.groupwallet.groupservice.util.StreamValidationResult;
import com.chaiweijian.groupwallet.groupservice.v1.Group;
import com.chaiweijian.groupwallet.groupservice.v1.UndeleteGroupRequest;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Component
public class UndeleteGroupRequestProcessor {
    private final KafkaProtobufSerde<Group> groupSerde;

    public UndeleteGroupRequestProcessor(KafkaProtobufSerde<Group> groupSerde) {
        this.groupSerde = groupSerde;
    }

    @Bean
    public Function<KStream<String, UndeleteGroupRequest>, Function<GlobalKTable<String, Group>,KStream<String, Status>>> undeleteGroup() {
        return undeleteGroupRequest -> groupAggregateStore -> {
            var groupExistsValidation = validateGroupExists(
                    undeleteGroupRequest,
                    groupAggregateStore);

            var tobeUndeletedGroup = groupExistsValidation
                    .getPassedStream()
                    .leftJoin(groupAggregateStore,
                            (leftKey, leftValue) -> leftKey,
                            (leftValue, rightValue) -> rightValue);

            var undeletedGroup = tobeUndeletedGroup
                    .mapValues(value -> value.toBuilder()
                            .clearDeleteTime()
                            .clearExpireTime()
                            .setState(Group.State.ACTIVE)
                            .setAggregateVersion(value.getAggregateVersion() + 1)
                            .setEtag(GroupAggregateUtil.calculateEtag(value.getAggregateVersion() + 1))
                            .build());

            undeletedGroup.to("groupwallet.groupservice.GroupUndeleted-events", Produced.with(Serdes.String(), groupSerde));

            var successStatus = undeletedGroup.mapValues(value -> OkStatusUtil.packStatus(value, "Group undeleted."));

            return groupExistsValidation.getStatusStream()
                    .merge(successStatus);
        };
    }

    private static StreamValidationResult<String, UndeleteGroupRequest> validateGroupExists(KStream<String, UndeleteGroupRequest> input, GlobalKTable<String, Group> groupAggregateStore) {
        var validation = input
                .leftJoin(groupAggregateStore,
                        (leftKey, leftValue) -> leftKey,
                        RequestAndExistingGroup::new);

        var failed = validation
                .filterNot((key, value) -> value.currentGroupExists())
                .mapValues(RequestAndExistingGroup::getRequest);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.NOT_FOUND_VALUE)
                        .setMessage(String.format("Group with name %s does not exists.", value.getName()))
                        .build());

        var passed = validation
                .filter(((key, value) -> value.currentGroupExists()))
                .mapValues(RequestAndExistingGroup::getRequest);

        return new StreamValidationResult<>(passed, failed, status);
    }
}
