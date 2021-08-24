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

package com.chaiweijian.groupwallet.groupservice.create;

import com.chaiweijian.groupwallet.groupservice.util.*;
import com.chaiweijian.groupwallet.groupservice.v1.CreateGroupRequest;
import com.chaiweijian.groupwallet.groupservice.v1.Group;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.google.rpc.BadRequest;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.Data;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.function.BiFunction;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.lang.System.currentTimeMillis;

@Component
public class CreateGroupRequestProcessor {

    private final KafkaProtobufSerde<Group> groupSerde;

    public CreateGroupRequestProcessor(KafkaProtobufSerde<Group> groupSerde) {
        this.groupSerde = groupSerde;
    }

    @Bean
    public BiFunction<KStream<String, CreateGroupRequest>, GlobalKTable<String, User>, KStream<String, Status>> createGroup() {
        return (createGroupRequest, userAggregateStore) -> {

            var joinedCreateGroupRequestAndOwner = createGroupRequest.leftJoin(
                    userAggregateStore,
                    (leftKey, leftValue) -> leftValue.getGroup().getOwner(),
                    CreateGroupRequestAndOwner::new);

            var ownerValidation = validateOwner(joinedCreateGroupRequestAndOwner);

            var formattedGroup = ownerValidation
                    .getPassedStream()
                    .mapValues(value -> SimpleGroupFormatter.format(value.getRequest().getGroup()));

            var simpleValidation = GroupStreamValidatorUtil.validateSimple(formattedGroup);

            var newGroup = simpleValidation.getPassedStream()
                    .mapValues(value -> value.toBuilder()
                            .setName(String.format("groups/%s", UUID.randomUUID()))
                            .setCreateTime(fromMillis(currentTimeMillis()))
                            .setState(Group.State.ACTIVE)
                            .setAggregateVersion(1)
                            .setEtag(GroupAggregateUtil.calculateEtag(value.getName(), 1))
                            .build());

            newGroup
                    .selectKey((key, value) -> value.getName())
                    .repartition(Repartitioned.with(Serdes.String(), groupSerde))
                    .to("groupwallet.groupservice.GroupCreated-events", Produced.with(Serdes.String(), groupSerde));

            var successStatus = newGroup
                    .mapValues(value -> OkStatusUtil.packStatus(value, "Group created."));

            return ownerValidation.getStatusStream()
                    .merge(simpleValidation.getStatusStream())
                    .merge(successStatus);
        };
    }

    private StreamValidationResult<String, CreateGroupRequestAndOwner> validateOwner(KStream<String, CreateGroupRequestAndOwner> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setFail(value.getOwner() == null));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem)
                .mapValues(value -> BadRequest.newBuilder()
                        .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                                .setField("owner")
                                .setDescription(String.format("%s does not exists.", value.getRequest().getGroup().getOwner()))
                                .build())
                        .build())
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filter(((key, value) -> value.isPassed()))
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    @Data
    private static class CreateGroupRequestAndOwner {
        private final CreateGroupRequest request;
        private final User owner;
    }
}
