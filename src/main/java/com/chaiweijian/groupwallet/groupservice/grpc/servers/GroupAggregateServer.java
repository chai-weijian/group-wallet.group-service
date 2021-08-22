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

package com.chaiweijian.groupwallet.groupservice.grpc.servers;

import com.chaiweijian.groupwallet.groupservice.v1.CreateGroupRequest;
import com.chaiweijian.groupwallet.groupservice.v1.GetGroupRequest;
import com.chaiweijian.groupwallet.groupservice.v1.Group;
import com.chaiweijian.groupwallet.groupservice.v1.GroupAggregateServiceGrpc;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.StatusException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import com.chaiweijian.groupwallet.groupservice.v1.UpdateGroupRequest;

import java.util.concurrent.TimeUnit;

@GrpcService
@Slf4j
public class GroupAggregateServer extends GroupAggregateServiceGrpc.GroupAggregateServiceImplBase {

    private final ReplyingKafkaTemplate<String, UpdateGroupRequest, Status> updateGroupTemplate;
    private final ReplyingKafkaTemplate<String, CreateGroupRequest, Status> createGroupTemplate;
    private final InteractiveQueryService interactiveQueryService;

    public GroupAggregateServer(ReplyingKafkaTemplate<String, UpdateGroupRequest, Status> updateGroupTemplate,
                                ReplyingKafkaTemplate<String, CreateGroupRequest, Status> createGroupTemplate,
                                InteractiveQueryService interactiveQueryService) {
        this.updateGroupTemplate = updateGroupTemplate;
        this.createGroupTemplate = createGroupTemplate;
        this.interactiveQueryService = interactiveQueryService;
    }

    @Override
    public void getGroup(GetGroupRequest request, StreamObserver<Group> responseObserver) {
        final ReadOnlyKeyValueStore<String, Group> groupAggregateStore
                = interactiveQueryService.getQueryableStore("groupwallet.groupservice.GroupAggregate-store", QueryableStoreTypes.keyValueStore());

        var group = groupAggregateStore.get(request.getName());

        if (group != null) {
            responseObserver.onNext(group);
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(StatusProto.toStatusRuntimeException(
                    Status.newBuilder()
                            .setCode(Code.NOT_FOUND_VALUE)
                            .setMessage(String.format("%s does not exists.", request.getName()))
                            .build()));
        }
    }

    @Override
    public void createGroup(CreateGroupRequest request, StreamObserver<Group> responseObserver) {
        ProducerRecord<String, CreateGroupRequest> record = new ProducerRecord<>(
                "groupwallet.groupservice.CreateGroup-requests",
                request.getGroup().getOwner(),
                request);

        RequestReplyFuture<String, CreateGroupRequest, Status> replyFuture = createGroupTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
            handleCreateUpdateResponse(consumerRecord, responseObserver);
        } catch (Exception exception) {
            log.error("GroupAggregateServer - createGroup Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    @Override
    public void updateGroup(UpdateGroupRequest request, StreamObserver<Group> responseObserver) {
        ProducerRecord<String, UpdateGroupRequest> record = new ProducerRecord<>(
                "groupwallet.groupservice.UpdateGroup-requests",
                request.getGroup().getName(),
                request);

        RequestReplyFuture<String, UpdateGroupRequest, Status> replyFuture = updateGroupTemplate.sendAndReceive(record);
        try {
            ConsumerRecord<String, Status> consumerRecord = replyFuture.get(10, TimeUnit.SECONDS);
            handleCreateUpdateResponse(consumerRecord, responseObserver);
        } catch (Exception exception) {
            log.error("GroupAggregateServer - updateGroup Error", exception);
            responseObserver.onError(new StatusException(io.grpc.Status.INTERNAL.withCause(exception)));
        }
    }

    private void handleCreateUpdateResponse(ConsumerRecord<String, Status> consumerRecord,
                                            StreamObserver<Group> responseObserver) throws InvalidProtocolBufferException {
        if (consumerRecord.value().getCode() == Code.OK_VALUE) {
            // if the response is not error, the first detail will be the group created/updated.
            Any detail = consumerRecord.value().getDetails(0);
            responseObserver.onNext(detail.unpack(Group.class));
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(StatusProto.toStatusRuntimeException(consumerRecord.value()));
        }
    }
}
