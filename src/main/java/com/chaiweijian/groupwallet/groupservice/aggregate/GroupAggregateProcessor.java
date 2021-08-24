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
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.chaiweijian.groupwallet.groupservice.v1.Group;

import java.util.function.Function;

@Component
public class GroupAggregateProcessor {
    private final KafkaProtobufSerde<Group> groupSerde;

    public GroupAggregateProcessor(KafkaProtobufSerde<Group> groupSerde) {
        this.groupSerde = groupSerde;
    }

    @Bean
    public Function<KStream<String, Group>, Function<KStream<String, Group>, Function<KStream<String, Group>, Function<KStream<String, Group>, KStream<String, Group>>>>> aggregateGroup() {
        return groupCreated -> groupUpdated -> groupDeleted -> groupUndeleted -> {
            var groupCreatedEvent = groupCreated.groupByKey();
            var groupUpdatedEvent = groupUpdated.groupByKey();
            var groupDeletedEvent = groupDeleted.groupByKey();
            var groupUndeletedEvent = groupUndeleted.groupByKey();

            return groupCreatedEvent
                    .cogroup(EventHandler::handleGroupCreatedEvent)
                    .cogroup(groupUpdatedEvent, EventHandler::handleGroupUpdatedEvent)
                    .cogroup(groupDeletedEvent, EventHandler::handleGroupDeletedEvent)
                    .cogroup(groupUndeletedEvent, EventHandler::handleGroupUndeletedEvent)
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
    }
}
