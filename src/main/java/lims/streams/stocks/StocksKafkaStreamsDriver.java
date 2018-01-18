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

package lims.streams.stocks;

import lims.model.StockTransaction;
import lims.model.StockTransactionCollector;
import lims.serializer.JsonDeserializer;
import lims.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;


public class StocksKafkaStreamsDriver {

    public static void main(String[] args) {

        StreamsConfig streamingConfig = new StreamsConfig(getProperties());

        JsonSerializer<StockTransactionCollector> stockTransactionsSerializer = new JsonSerializer<>();
        JsonDeserializer<StockTransactionCollector> stockTransactionsDeserializer = new JsonDeserializer<>(StockTransactionCollector.class);
        JsonDeserializer<StockTransaction> stockTxnDeserializer = new JsonDeserializer<>(StockTransaction.class);
        JsonSerializer<StockTransaction> stockTxnJsonSerializer = new JsonSerializer<>();
        Serde<StockTransaction> transactionSerde = Serdes.serdeFrom(stockTxnJsonSerializer,stockTxnDeserializer);
        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        Serde<String> stringSerde = Serdes.serdeFrom(stringSerializer,stringDeserializer);
        Serde<StockTransactionCollector> collectorSerde = Serdes.serdeFrom(stockTransactionsSerializer,stockTransactionsDeserializer);
        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

//根据一个指定的topic创建KStream。消费这个topic 上的消息，并且把结果forward 到下游processor/sink节点,开启一个source--->processor---->sink 的topology
        KStream<String,StockTransaction> transactionKStream =  kStreamBuilder.stream(stringSerde,transactionSerde,"stocks");

        transactionKStream.map((k,v)-> new KeyValue<>(v.getSymbol(),v))//map the StockTransaction （没有key）to一个新的keyValue对象,把ticker symbol作为新对象的key
                          .through(stringSerde, transactionSerde,"stocks-out") //Materialize this stream(transactionSerde) to a topic(stocks-out)/把这个流写入 topic stocks-out
                          .groupBy((k,v) -> k, stringSerde, transactionSerde)//聚合之前，分组是必须的
                          .aggregate(StockTransactionCollector::new,
                               (k, v, stockTransactionCollector) -> stockTransactionCollector.add(v),
                               TimeWindows.of(10000),
                               collectorSerde, "stock-summaries")
                .to(windowedSerde,collectorSerde,"transaction-summary");
//        transactionKStream.through(stringSerde, transactionSerde,"stocks-out")
//                .map((k,v)-> new KeyValue<>(v.getSymbol(),v))
//                .aggregateByKey(StockTransactionCollector::new,
//                        (k, v, stockTransactionCollector) -> stockTransactionCollector.add(v),
//                        TumblingWindows.of("stock-summaries").with(10000),
//                        stringSerde,collectorSerde)
//                .to(windowedSerde,collectorSerde,"transaction-summary");


        System.out.println("Starting StockStreams Example");
        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,streamingConfig);
        kafkaStreams.start();
        System.out.println("Now started StockStreams Example");

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Stocks-Streams-Processor");
        props.put("group.id", "stock-streams");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stocks_streams_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
