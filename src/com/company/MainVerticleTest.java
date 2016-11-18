package com.company;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import io.vertx.core.Vertx;
import io.vertx.core.cli.converters.BooleanConverter;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.xpath.operations.Bool;
import org.jruby.RubyProcess;
import org.jruby.javasupport.ext.JavaLang;
import org.junit.*;
import org.junit.runner.RunWith;

import org.openqa.selenium.*;
import org.openqa.selenium.chrome.ChromeDriver;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.Assert.*;

/**
 * Created by SalmonKiller on 11/3/16.
 */

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {

    Vertx vertx;
    Integer port = 8000;
    String body_test = "lol hi";
    String title_test = "A post lel";
    String private_test ="false";

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp() throws Exception {
            vertx = Vertx.vertx();
            vertx.deployVerticle(MainVerticle.class.getName());
    }

    @After
    public void tearDown(TestContext context) throws Exception {
        vertx.close(context.asyncAssertSuccess());
    }

    @Ignore
    public void start(TestContext context) throws Exception {
        TestSuite suite = TestSuite.create("test_suite");
        suite.test("basic_insert", context1 -> {
            basicInsertTest(context1);
        }).test("missing_param", context1 -> {
           missingParametersTest(context1);
        });
    }

    @Test
    public void missingParametersTest(TestContext context) {
        Async async = context.async();
        Timestamp expires = generateTimestamp(100);

        String insertion_string = "";

        try {

            insertion_string = "title=" + URLEncoder.encode(title_test, "UTF-8");
            insertion_string += "&private=" + URLEncoder.encode(private_test, "UTF-8")+"&expires="+
                    URLEncoder.encode(expires.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getStackTrace());
        }

        vertx.createHttpClient().post(port, "localhost", "/entries/")
                .putHeader("content-type", "application/x-www-form-urlencoded")
                .putHeader("content-length", ""+insertion_string.length())
                .handler(httpClientResponse -> {
                    httpClientResponse.bodyHandler(body -> {
                        String entry_id = body.toString();
                        //System.out.println(entry_id);
                        assertEquals(entry_id, MainVerticle.getMissing_params_message());
                        async.complete();
                    });
                })
                .write(insertion_string)
                .end();
    }

    private Timestamp generateTimestamp(Integer secs) {
        Long seconds = Integer.toUnsignedLong(secs);
        Timestamp expires = new Timestamp(System.currentTimeMillis()+seconds*1000);
        return expires;
    }

    @Test
    public void basicInsertTest(TestContext context) {
        //WebDriver driver = new ChromeDriver();
        Async async = context.async();

        Timestamp expires = generateTimestamp(100);

        String insertion_string = "";

        try {

            insertion_string = "body=" + URLEncoder.encode(body_test, "UTF-8") + "&title=" + URLEncoder.encode(title_test, "UTF-8");
            insertion_string += "&private=" + URLEncoder.encode(private_test, "UTF-8")+"&expires="+
                    URLEncoder.encode(expires.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getStackTrace());
        }

        vertx.createHttpClient().post(port, "localhost", "/entries/")
                .putHeader("content-type", "application/x-www-form-urlencoded")
                .putHeader("content-length", ""+insertion_string.length())
                .handler(httpClientResponse -> {
                    httpClientResponse.bodyHandler(body -> {
                        String entry_id = body.toString();
                        //System.out.println(entry_id);
                        Boolean x = checkIfExistsInDatabase(entry_id, "public");
                        //System.out.println(x);
                        assertTrue(x);
                        async.complete();
                    });
                })
                .write(insertion_string)
                .end();

    }

    public boolean checkIfExistsInDatabase(String entry_id, String table_name) {
        Cluster cluster = null;
        cluster = Cluster.builder()                                                    // (1)
                .addContactPoint("127.0.0.1")
                .build();
        Boolean return_value = false;
        try {
            Session session = cluster.connect();
            ResultSet resultSet = session.execute("select * from entry_keyspace.entries_table_" + table_name + " where entry_id=" +
                    entry_id + ";");

            if (!resultSet.isExhausted()) {
                return_value = true;
            }

            return return_value;


        } finally {
            if (cluster!=null) {cluster.close();}
        }

    }

}