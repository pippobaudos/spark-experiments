package com.ronciao.experiment.elasticsearchSparkReadTest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.DeleteIndex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SparkElasticsearchTest {

    final static String HOST = "localhost";
    final static String PORT = "9200";
    final static String INDEX = "spark";
    final static String INDEX_TYPE = INDEX + "/doc";

    JavaSparkContext jsc;

    @Before
    public void setup() {
        SparkConf sparkConf = new SparkConf().setAppName("JavaLogQuery").setMaster("local[4]");
        sparkConf.set("es.resource", INDEX_TYPE);
        sparkConf.set("es.query", "{\"query\":{\"match_all\":{}}}");
        sparkConf.set("es.nodes", HOST);
        sparkConf.set("es.port", PORT);

        this.jsc = new JavaSparkContext(sparkConf);
    }

    @Test
    public void writeReadSparkEs_insert4DocsAndRead_VerifyNumDocsInserted() {

        try {
            deleteElasticsearchIndex();
        }
        catch (IOException e) {
            fail("Not possible to connect to the ELASTICSEARCH instance: " + HOST + ":" + PORT + ". Is it RUNNING???");
        }


        insert2DocsWithSaveToEs();
        Insert2DocsWithSaveJsonToEs();

        JavaPairRDD<String, Map<String, Object>> rddReadFromElastic = JavaEsSpark.esRDD(jsc, INDEX_TYPE);

        assertEquals(4, rddReadFromElastic.count());
    }

    @After
    public void end() {
        jsc.stop();
    }

    private void Insert2DocsWithSaveJsonToEs() {
        String json1 = "{" +
                        "  \"_id\": \"123\"," +
                        "  \"title\": \"The Godfather\"," +
                        "  \"director\": \"Francis Ford Coppola\"," +
                        "  \"year\": 1972" +
                        "}";
        String json2 = "{" +
                        "  \"_id\": \"1234\"," +
                        "  \"title\": \"Star Wards\"," +
                        "  \"director\": \"George Lucas\"," +
                        "  \"year\": 1978" +
                        "}";

        JavaRDD<String> jsonRDD = jsc.parallelize(Arrays.asList(json1, json2));
        JavaEsSpark.saveJsonToEs(jsonRDD, INDEX_TYPE, ImmutableMap.of("es.mapping.id", "_id", "es.mapping.exclude", "_id"));
    }

    private void insert2DocsWithSaveToEs() {
        Map<String, ?> doc1 = ImmutableMap.of("one", 1, "two", 2.4f);
        Map<String, ?> doc2 = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");
        JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(doc1, doc2));
        JavaEsSpark.saveToEs(javaRDD, INDEX_TYPE);
    }

    private static void deleteElasticsearchIndex() throws IOException {
        JestClient client = buildJestClient();
        client.execute(new DeleteIndex.Builder(INDEX).build());
    }

    public static JestClient buildJestClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://" +HOST+":"+PORT)
                .multiThreaded(true)
                .build());
        return factory.getObject();
    }


}