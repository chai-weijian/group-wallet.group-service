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

package com.chaiweijian.groupwallet.groupservice.update;

import com.chaiweijian.groupwallet.groupservice.util.*;
import com.chaiweijian.groupwallet.groupservice.v1.Group;
import com.chaiweijian.groupwallet.groupservice.v1.UpdateGroupRequest;
import com.chaiweijian.groupwallet.userservice.v1.User;
import com.google.protobuf.Any;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.FieldMaskUtil;
import com.google.rpc.BadRequest;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import lombok.Data;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Function;

@Component
public class UpdateGroupRequestProcessor {
    private final KafkaProtobufSerde<Group> groupSerde;

    public UpdateGroupRequestProcessor(KafkaProtobufSerde<Group> groupSerde) {
        this.groupSerde = groupSerde;
    }

    @Bean
    public Function<KStream<String, UpdateGroupRequest>, Function<GlobalKTable<String, User>, Function<GlobalKTable<String, Group>, KStream<String, Status>>>> updateGroup() {
        return updateGroupRequest -> userAggregateStore -> groupAggregateStore -> {

            var updateGroupRequestAndOwner = updateGroupRequest.leftJoin(
                    userAggregateStore,
                    (leftKey, leftValue) -> leftValue.getGroup().getOwner(),
                    UpdateGroupRequestAndOwner::new);

            var ownerValidation = validateOwner(updateGroupRequestAndOwner);

            var groupExistsValidation = validateGroupExists(
                    ownerValidation.getPassedStream().mapValues(UpdateGroupRequestAndOwner::getRequest),
                    groupAggregateStore);

            var updateGroupRequestAndCurrentGroup = groupExistsValidation
                    .getPassedStream()
                    .leftJoin(groupAggregateStore,
                            (leftKey, leftValue) -> leftKey,
                            RequestAndExistingGroup::new);

            var etagValidation = validateEtag(updateGroupRequestAndCurrentGroup);

            var softDeleteValidation = validateSoftDelete(etagValidation.getPassedStream());

            var updatedFieldMask = softDeleteValidation.getPassedStream()
                    .mapValues(value -> new RequestAndExistingGroup<>(
                            value.getRequest().toBuilder().setUpdateMask(removeImmutableField(value.getRequest().getUpdateMask())).build(),
                            value.getCurrentGroup()));

            var fieldMaskValidation = validateFieldMask(updatedFieldMask);

            var mergedGroup = fieldMaskValidation.getPassedStream()
                    .mapValues(value -> {
                        var result = value.getCurrentGroup().toBuilder();
                        FieldMaskUtil.merge(value.getRequest().getUpdateMask(), value.getRequest().getGroup(), result);
                        return SimpleGroupFormatter.format(result.build());
                    });

            var simpleValidation = GroupStreamValidatorUtil.validateSimple(mergedGroup);

            var updatedGroup = simpleValidation
                    .getPassedStream()
                    .mapValues(value -> value.toBuilder()
                            .setAggregateVersion(value.getAggregateVersion() + 1)
                            .setEtag(GroupAggregateUtil.calculateEtag(value.getName(), value.getAggregateVersion() + 1))
                            .build());

            updatedGroup.to("groupwallet.groupservice.GroupUpdated-events", Produced.with(Serdes.String(), groupSerde));

            var successStatus = updatedGroup.mapValues(value -> OkStatusUtil.packStatus(value, "Group updated."));

            return ownerValidation.getStatusStream()
                    .merge(groupExistsValidation.getStatusStream())
                    .merge(etagValidation.getStatusStream())
                    .merge(softDeleteValidation.getStatusStream())
                    .merge(fieldMaskValidation.getStatusStream())
                    .merge(simpleValidation.getStatusStream())
                    .merge(successStatus);
        };
    }

    private StreamValidationResult<String, UpdateGroupRequestAndOwner> validateOwner(KStream<String, UpdateGroupRequestAndOwner> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setFail(value.getOwner() == null));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> BadRequest.newBuilder()
                        .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                                .setField("owner")
                                .setDescription(String.format("%s does not exists.", value.getRequest().getGroup().getOwner()))
                                .build())
                        .build())
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filter((key, value) -> value.isPassed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }


    private static StreamValidationResult<String, UpdateGroupRequest> validateGroupExists(KStream<String, UpdateGroupRequest> input, GlobalKTable<String, Group> groupAggregateStore) {
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
                        .setMessage(String.format("Group with name %s does not exists.", value.getGroup().getName()))
                        .build());

        var passed = validation
                .filter(((key, value) -> value.currentGroupExists()))
                .mapValues(RequestAndExistingGroup::getRequest);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, RequestAndExistingGroup<UpdateGroupRequest>> validateEtag(KStream<String, RequestAndExistingGroup<UpdateGroupRequest>> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(value.getRequest().getGroup().getEtag().equals(value.getCurrentGroup().getEtag())));

        var failed = validation
                .filter((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.ABORTED_VALUE)
                        .setMessage("Concurrency error.")
                        .addDetails(Any.pack(ErrorInfo.newBuilder()
                                .setReason("Etag is not the latest version.")
                                .setDomain("groupservice.groupwallet.chaiweijian.com")
                                .putMetadata("providedEtag", value.getRequest().getGroup().getEtag())
                                .build()))
                        .build());

        var passed = validation
                .filterNot((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private StreamValidationResult<String, RequestAndExistingGroup<UpdateGroupRequest>> validateSoftDelete(KStream<String, RequestAndExistingGroup<UpdateGroupRequest>> input) {
        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setFail(value.getCurrentGroup().getState() == Group.State.DELETED));

        var failed = validation
                .filter((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        var status = failed
                .mapValues(value -> Status.newBuilder()
                        .setCode(Code.FAILED_PRECONDITION_VALUE)
                        .setMessage("Group is in DELETED state.")
                        .addDetails(Any.pack(ErrorInfo.newBuilder()
                                .setReason("Group is soft deleted.")
                                .setDomain("groupservice.groupwallet.chaiweijian.com")
                                .putMetadata("name", value.getRequest().getGroup().getName())
                                .build()))
                        .build());

        var passed = validation
                .filterNot((key, value) -> value.isFailed())
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    private static FieldMask removeImmutableField(FieldMask fieldMask) {
        final FieldMask OUTPUT_ONLY = FieldMask.newBuilder()
                .addPaths("name")
                .addPaths("state")
                .addPaths("create_time")
                .addPaths("delete_time")
                .addPaths("expire_time")
                .addPaths("aggregate_version")
                .addPaths("etag")
                .build();

        return FieldMaskUtil.subtract(fieldMask, OUTPUT_ONLY);
    }

    private StreamValidationResult<String, RequestAndExistingGroup<UpdateGroupRequest>> validateFieldMask(KStream<String, RequestAndExistingGroup<UpdateGroupRequest>> input) {

        var validation = input
                .mapValues(value -> new ValidationResult<>(value).setPass(FieldMaskUtil.isValid(Group.class, value.getRequest().getUpdateMask())));

        var failed = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        var status = validation
                .filter(((key, value) -> value.isFailed()))
                .mapValues(value -> BadRequest.newBuilder()
                        .addFieldViolations(BadRequest.FieldViolation.newBuilder()
                                .setField("update_mask")
                                .setDescription("Unable to map update_mask to User type."))
                        .build())
                .mapValues(BadRequestUtil::packStatus);

        var passed = validation
                .filterNot(((key, value) -> value.isFailed()))
                .mapValues(ValidationResult::getItem);

        return new StreamValidationResult<>(passed, failed, status);
    }

    @Data
    private static class UpdateGroupRequestAndOwner {
        private final UpdateGroupRequest request;
        private final User owner;
    }
}
