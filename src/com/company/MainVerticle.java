package com.company;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.uuid.Generators;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import org.apache.xpath.operations.Bool;
import org.jruby.RubyProcess;
import org.junit.runner.RunWith;

import javax.print.DocFlavor;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;

import java.sql.Array;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by SalmonKiller on 10/22/16.
 */
public class MainVerticle extends AbstractVerticle {
    HttpServer server;
    EventBus eb;
    static String missing_params_message = "Sorry, some of the essential parameters are missing.";
    @Override
    public void start() throws Exception {
        server = vertx.createHttpServer();

        eb = vertx.eventBus();

        Router router = Router.router(vertx);

        System.out.println(new File("").getCanonicalPath());

        router.post("/entries/").consumes("application/x-www-form-urlencoded").handler(routingContext -> {
            System.out.println("reached application form urlunencoded");
            HttpServerRequest request = routingContext.request();
            String type = request.getHeader("content-type");
            String[] return_array = {null, null, null, null}; // body, title, expire, private
            Map<String, String> map = new HashMap<String, String>();

                    request.bodyHandler(data -> {

                        try {
                            String decoded_string = URLDecoder.decode(data.toString(), "utf-8");
//                            System.out.println(data.toString());
//                            System.out.println(decoded_string);
                            String[] x = decoded_string.split("\\&");
                            for (int i=0; i<x.length; i++) {
                                //System.out.println(x[i]);
                                String[] param = x[i].split("\\=");
                                map.put(param[0], param[1]);
                            }
                            if (map.get("body") == null || map.get("title") == null) {
                                System.out.println("throwing exception");
                                throw new RequiredParamsMissingException();
                            }

                            insert_values(map.get("body"), map.get("title"),map.get("expires"), map.get("private"), routingContext);

                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        } catch (RequiredParamsMissingException e) {
                            routingContext.response().end(missing_params_message);
                        }


                    });


        });

        router.post("/entries/").handler(routingContext -> {

                    //System.out.println(routingContext.request().getHeader("content-type"));
                    String body = routingContext.request().getParam("body");
                    String title = routingContext.request().getParam("title");
                    String expires = routingContext.request().getParam("expires");
                    String private_string = routingContext.request().getParam("private");

                    //System.out.println(body);

                    // ******** Make Try catches for the errors that might happen with invalid data ********

                    insert_values(body, title, expires, private_string, routingContext);
        });




        router.get("/entries/").handler(routingContext -> {
            Cluster cluster = null;
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .build();

           try {

               Session session = cluster.connect();
               ResultSet resultSet = session.execute("select * from entry_keyspace.entries_table_public;");

               final String[] output_string = {""};
               resultSet.forEach(row -> {
                   String myString = "\n"+row.getString("body");
                  // System.out.println(myString);
                    output_string[0] = output_string[0] + myString;
               });

               HttpServerResponse response = routingContext.response();
               response.putHeader("content-type", "text/plain");

               response.end(output_string[0]);
           } finally {
               if (cluster != null) cluster.close();
           }



        });

        router.put("/entries/:entry_id/").handler(routingContext -> {
            String entry_id = routingContext.request().getParam("entry_id");
            String body = routingContext.request().getParam("body");
            String title = routingContext.request().getParam("title");
            String expires = routingContext.request().getParam("expires");
            String private_string = routingContext.request().getParam("private");

            //System.out.println("someone wanted to put something");


            HttpServerResponse response = routingContext.response();
            response.putHeader("content-type", "text/plain");
            response.end("");

            Cluster cluster = null;
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .build();

            try {
                Session session = cluster.connect();

                ResultSet public_result_set = session.execute(get_execution_string(entry_id, "public"));

                ResultSet private_result_set = session.execute(get_execution_string(entry_id, "private"));

                ResultSet process_set = null;
                String old_table_name = "";

                if (!public_result_set.isExhausted()) {
                    process_set = public_result_set;
                    old_table_name = "public";
                } else if (!private_result_set.isExhausted()) {
                    process_set = private_result_set;
                    old_table_name = "private";
                } else {
                    // ****** Throw exception that entry was not found
                }

                //Row entry_row = process_set.one();

                String new_table_name = "public";
                if (private_string.equals("true"))
                    new_table_name = "private";

                if (new_table_name.equals(old_table_name)) {
                    // ****** Use Update
                    String execution_string = get_update_statement(entry_id, "entries_table_"+new_table_name, body, title,
                            get_expiration_secs(expires));
                    session.execute(execution_string);
                } else {
                   // ******** Use Delete and Insert
                    String delete_string = get_deletion_statement("entries_table_"+old_table_name, entry_id);
                    session.execute(delete_string);

                    String insert_string = get_insertion_statement(entry_id, "entries_table_"+new_table_name, body, title,
                            get_expiration_secs(expires));
                    session.execute(insert_string);
                }

            } finally {
                if (cluster != null) cluster.close();
            }
        });

        router.delete("/entries/:entry_id/").handler(routingContext -> {
            String entry_id = routingContext.request().getParam("entry_id");

            HttpServerResponse response = routingContext.response();
            response.putHeader("content-type", "text/plain");
            response.end("");

            Cluster cluster = null;
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("127.0.0.1")
                    .build();

            try {
                Session session = cluster.connect();

                ResultSet public_result_set = session.execute(get_execution_string(entry_id, "public"));

                ResultSet private_result_set = session.execute(get_execution_string(entry_id, "private"));

                if (!public_result_set.isExhausted()) {
                    String delete_string = get_deletion_statement("entries_table_public", entry_id);
                    session.execute(delete_string);
                } else if (!private_result_set.isExhausted()) {
                    String delete_string = get_deletion_statement("entries_table_private", entry_id);
                    session.execute(delete_string);
                } else {
                    // ****** Throw exception that entry was not found
                }
            } finally {
                if (cluster!=null) cluster.close();
            }
        });

        SockJSHandlerOptions options = new SockJSHandlerOptions().setHeartbeatInterval(2000);

        SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);

        sockJSHandler.socketHandler(sockJSSocket -> {

            // Just echo the data back


            eb.consumer("new_public_message", message -> {
                JsonArray r = (JsonArray) message.body();
//                StringBuffer buffer = new StringBuffer(r.getString(0));
//                sockJSSocket.write(buffer);
            });
            //sockJSSocket.handler();
        });

        router.route("/static/*").handler(StaticHandler.create("webroot"));

        router.route("/latest/*").handler(sockJSHandler);


        server.requestHandler(router::accept).listen(8000);


    }

    private Double get_expiration_secs(String expires) {
        Timestamp timestamp = Timestamp.valueOf(expires);
        timestamp = new Timestamp(timestamp.getTime() - System.currentTimeMillis());
        return Double.valueOf(timestamp.getTime()/1000.0);
    }

    private String get_insertion_statement(String code, String table_name, String body, String title) {
        // ***** handle optional entries here
        return "insert into entry_keyspace."+table_name+" (entry_id, body, title) " +
                "values ("+code+", '"+body+"', '"+title+"') ;";
    }

    private String get_insertion_statement(String code, String table_name, String body, String title, Double expirationTime) {
        // ***** handle optional entries here
        return "insert into entry_keyspace."+table_name+" (entry_id, body, title) " +
                "values ("+code+", '"+body+"', '"+title+"') using ttl "+expirationTime.intValue()+";";
    }

    private String get_update_statement(String entry_id, String table_name, String body, String title, Double expirationTime) {
        // ***** handle optional entries here
        return "update entry_keyspace."+table_name+" using ttl "+expirationTime.intValue()+" set body='"+body+
                "', title='"+title+"' where entry_id="+entry_id+";";
    }

    private String get_deletion_statement(String table_name, String entry_id) {
        return "delete from entry_keyspace."+table_name+" where entry_id="+entry_id+";";
    }

    private String get_execution_string(String entry_id, String data_set) { // data_set can be either 'public' or 'private'
        String return_string = "select * from entry_keyspace.entries_table_"+data_set+" where entry_id="+entry_id+";";
        return return_string;
    }

    private void insert_data() {
        // ********** Maybe something will go here
    }

    private void insert_values(String body, String title, String expires, String private_string, RoutingContext routingContext) {
        UUID uuid = Generators.timeBasedGenerator().generate();

        Cluster cluster = null;
        cluster = Cluster.builder()                                                    // (1)
                .addContactPoint("127.0.0.1")
                .build();
        try {
            Session session = cluster.connect(); // (2)

            String table_name = "entries_table_public";
            if (private_string.equals("true")) {
                table_name = "entries_table_private";
            }


//                System.out.println(new Timestamp(System.currentTimeMillis()).toString());
//                System.out.println(Timestamp.valueOf(expires).toString());
//                System.out.println(expirationTime);


            String executionString = get_insertion_statement(uuid.toString(), table_name, body, title, get_expiration_secs(expires));
            session.execute(executionString);
            if (private_string.equals("false")) {
                //String[] array = {body, title, expires};
                JsonArray array = new JsonArray();
                array.add(body);
                array.add(title);
                array.add(expires);
                eb.publish("new_public_message", array);
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        finally {
            if (cluster != null) cluster.close();                                          // (5)
        }
        routingContext.response().end(uuid.toString());
    };

    @FunctionalInterface
    public interface ThrowingConsumer<T> extends Handler<T> {

        @Override
        default void handle(final T elem) {
            try {
                acceptThrows(elem);
            } catch (final RequiredParamsMissingException e) {
                // Implement your own exception handling logic here..
                // For example:
                //System.out.println("handling an exception...");
                // Or ...
                throw e;
            }
        }

        void acceptThrows(T elem) throws RequiredParamsMissingException;

    }

    public static String getMissing_params_message() {
        return missing_params_message;
    }

}

