/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lims.streams.purchases;

import lims.model.Purchase;
import lims.model.PurchasePattern;
import lims.model.RewardAccumulator;
import lims.serializer.JsonDeserializer;
import lims.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;


public class PurchaseKafkaStreamsDriver {

    public static void main(String[] args) {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
//构造 Purchase 的serde
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer,purchaseJsonDeserializer);
//构造 RewardAccumulator的serde
        JsonSerializer<RewardAccumulator> rewardAccumulatorJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<RewardAccumulator> rewardAccumulatorJsonDeserializer = new JsonDeserializer<>(RewardAccumulator.class);
        Serde<RewardAccumulator> rewardAccumulatorSerde = Serdes.serdeFrom(rewardAccumulatorJsonSerializer,rewardAccumulatorJsonDeserializer);
//构造 PurchasePattern的serde
        JsonSerializer<PurchasePattern> purchasePatternJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<PurchasePattern> purchasePatternJsonDeserializer = new JsonDeserializer<>(PurchasePattern.class);
        Serde<PurchasePattern> purchasePatternSerde = Serdes.serdeFrom(purchasePatternJsonSerializer,purchasePatternJsonDeserializer);

        Serde<String> stringSerde = Serdes.String();

        final KStreamBuilder kStreamBuilder = new KStreamBuilder();
        //#1,CC Masking . 从src-topoc里消费数据，对每一条Purchase记录里的信用卡编号进行标记(maskCreditCard)
        KStream<String,Purchase> purchaseKStream = kStreamBuilder.stream(stringSerde,purchaseSerde,"src-topic")
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        //#2，接收从上游#1流过来的数据，对每一条Purchase记录，收集邮编和购买的物品，并写入/topic patterns
        purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build()).to(stringSerde,purchasePatternSerde,"patterns");
        //#3，接收从上游#1流过来的数据，对每一条Purchase记录，收集用户姓名和消费金额，并写入topic rewards
        purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build()).to(stringSerde,rewardAccumulatorSerde,"rewards");
        //#4，把#1处理后的数据写入topic purchases
        purchaseKStream.to(stringSerde,purchaseSerde,"purchases");

        System.out.println("Starting PurchaseStreams Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,streamsConfig);
        kafkaStreams.start();
        System.out.println("Now started PurchaseStreams Example");

    }




    private static Properties getProperties() {
        Properties props = new Properties();
      //  props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Kafka-Streams-Job");
        props.put("group.id", "streams-purchases");
     //   props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-streams-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.30.88.106:9092");
       // props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "172.30.88.106:2182");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
      //  props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

}
