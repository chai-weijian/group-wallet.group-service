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

package com.chaiweijian.groupwallet.groupservice.aggregate;

import com.chaiweijian.groupwallet.groupservice.util.GroupAggregateUtil;
import com.chaiweijian.groupwallet.groupservice.util.ResourceNameUtil;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import com.chaiweijian.groupwallet.userservice.v1.GroupInvitation;

import com.chaiweijian.groupwallet.groupservice.v1.Group;

import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class GroupAggregateProcessor {
    private final KafkaProtobufSerde<Group> groupSerde;
    private final KafkaProtobufSerde<GroupInvitation> groupInvitationSerde;

    public GroupAggregateProcessor(KafkaProtobufSerde<Group> groupSerde, KafkaProtobufSerde<GroupInvitation> groupInvitationSerde) {
        this.groupSerde = groupSerde;
        this.groupInvitationSerde = groupInvitationSerde;
    }

    @Bean
    public Function<KStream<String, Group>, Function<KStream<String, Group>, Function<KStream<String, Group>, Function<KStream<String, Group>,
            Function<KStream<String, GroupInvitation>, Function<KStream<String, String>, Function<KStream<String, String>, KStream<String, Group>>>>>>>> aggregateGroup() {
        return groupCreated -> groupUpdated -> groupDeleted -> groupUndeleted -> groupInvitationAccepted -> memberRemoved -> groupRemoved -> {
            var groupCreatedEvent = groupCreated.groupByKey();
            var groupUpdatedEvent = groupUpdated.groupByKey();
            var groupDeletedEvent = groupDeleted.groupByKey();
            var groupUndeletedEvent = groupUndeleted.groupByKey();
            var groupInvitationAcceptedEvent = groupInvitationAccepted
                    .selectKey(((key, value) -> value.getGroup()))
                    .repartition(Repartitioned.with(Serdes.String(), groupInvitationSerde))
                    .groupByKey();
            var memberRemovedEvent = memberRemoved.groupByKey();
            var groupRemovedEvent = groupRemoved
                    .map(((key, value) -> KeyValue.pair(value, key)))
                    .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
                    .groupByKey();

            return groupCreatedEvent
                    .cogroup(EventHandler::handleGroupCreatedEvent)
                    .cogroup(groupUpdatedEvent, EventHandler::handleGroupUpdatedEvent)
                    .cogroup(groupDeletedEvent, EventHandler::handleGroupDeletedEvent)
                    .cogroup(groupUndeletedEvent, EventHandler::handleGroupUndeletedEvent)
                    .cogroup(groupInvitationAcceptedEvent, EventHandler::handleGroupInvitationAcceptedEvent)
                    .cogroup(memberRemovedEvent, EventHandler::handleMemberRemovedEvent)
                    .cogroup(groupRemovedEvent, EventHandler::handleMemberRemovedEvent)
                    .aggregate(() -> null,
                            Materialized.<String, Group, KeyValueStore<Bytes, byte[]>>as("groupwallet.groupservice.GroupAggregate-store")
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(groupSerde))
                    .toStream();
        };
    }

    private static class EventHandler {
        public static Group handleGroupCreatedEvent(String key, Group group, Group init) {
            var aggregateVersion = 1;
            return group.toBuilder()
                    .setAggregateVersion(aggregateVersion)
                    .setEtag(GroupAggregateUtil.calculateEtag(group.getName(), aggregateVersion))
                    .build();
        }

        public static Group handleGroupUpdatedEvent(String key, Group group, Group init) {
            var aggregateVersion = init.getAggregateVersion() + 1;
            return group.toBuilder()
                    .setAggregateVersion(aggregateVersion)
                    .setEtag(GroupAggregateUtil.calculateEtag(group.getName(), aggregateVersion))
                    .build();
        }

        public static Group handleGroupDeletedEvent(String key, Group group, Group init) {
            var aggregateVersion = init.getAggregateVersion() + 1;
            return group.toBuilder()
                    .setAggregateVersion(aggregateVersion)
                    .setEtag(GroupAggregateUtil.calculateEtag(group.getName(), aggregateVersion))
                    .build();
        }

        public static Group handleGroupUndeletedEvent(String key, Group group, Group init) {
            var aggregateVersion = init.getAggregateVersion() + 1;
            return group.toBuilder()
                    .setAggregateVersion(aggregateVersion)
                    .setEtag(GroupAggregateUtil.calculateEtag(group.getName(), aggregateVersion))
                    .build();
        }

        public static Group handleGroupInvitationAcceptedEvent(String key, GroupInvitation groupInvitation, Group aggregate) {
            var aggregateVersion = aggregate.getAggregateVersion() + 1;
            return aggregate.toBuilder()
                    .setAggregateVersion(aggregateVersion)
                    .setEtag(GroupAggregateUtil.calculateEtag(aggregate.getName(), aggregateVersion))
                    .addMembers(ResourceNameUtil.getGroupInvitationParentName(groupInvitation.getName()))
                    .build();
        }

        public static Group handleMemberRemovedEvent(String key, String member, Group aggregate) {
            var aggregateVersion = aggregate.getAggregateVersion() + 1;
            return aggregate.toBuilder()
                    .setAggregateVersion(aggregateVersion)
                    .setEtag(GroupAggregateUtil.calculateEtag(aggregate.getName(), aggregateVersion))
                    .clearMembers()
                    .addAllMembers(aggregate.getMembersList().stream().filter(m -> !member.equals(m)).collect(Collectors.toList()))
                    .build();
        }
    }
}
