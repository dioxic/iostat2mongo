package uk.dioxic.iostat2mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.connection.netty.NettyStreamFactoryFactory;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.cli.*;
import org.bson.Document;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CliOptions {

    private MongoClient client;
    private MongoCollection<Document> collection;
    private MongoDatabase database;
    private int batchSize;
    private int threads;
    private Path path;
    private Document attributes;
    private String username;
    private String password;
    private String authenticationDatabase;
    private EventLoopGroup eventLoopGroup;

    public CliOptions(String[] args) {
        Options options = new Options();
        options.addOption("f", "path", true, "iostat path path");
        options.addOption("h", "uri", true, "mongodb uri");
        options.addOption("b","batchSize", true, "mongodb bulkwrite batch size");
        options.addOption("t", "threads", true, "writer threads (defaults to CPU core count)");
        options.addOption("d", "database", true, "mongodb database");
        options.addOption("c", "collection", true, "mongodb collection");
        options.addOption("x", "attributes", true, "additional attributes to add to the mongodb documents (expected JSON)");
        options.addOption("u", "username", true, "mongodb username");
        options.addOption("p", "password", true, "mongodb password");
        options.addOption("a", "authenticationDatabase", true, "mongodb authentication database");
        options.addOption("s", "ssl", false, "enable SSL");

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cli = parser.parse(options, args);

            MongoClientSettings.Builder mongoClientBuilder = MongoClientSettings.builder();
            mongoClientBuilder.applicationName("iostat loader");
            //mongoClientBuilder.applyToConnectionPoolSettings(builder -> builder.maxSize(10));

            username = cli.getOptionValue('u');
            password = cli.getOptionValue('p');
            authenticationDatabase = cli.getOptionValue('a', "admin");

            if (username != null && password != null) {
                mongoClientBuilder
                        .credential(MongoCredential.createCredential(username, authenticationDatabase, password.toCharArray()));
            }

            if (cli.hasOption('h')) {
                mongoClientBuilder.applyConnectionString(new ConnectionString(cli.getOptionValue('h')));
            }

            if (cli.hasOption('s')) {
                EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
                mongoClientBuilder
                        .streamFactoryFactory(NettyStreamFactoryFactory.builder()
                            .eventLoopGroup(eventLoopGroup).build());
            }

            client = MongoClients.create(mongoClientBuilder.build());

            database = client.getDatabase(cli.getOptionValue('d', "test"));
            collection = database.getCollection(cli.getOptionValue('c', "iostats"));
            batchSize = Integer.parseInt(cli.getOptionValue('b', "1000"));
            threads = Integer.parseInt(cli.getOptionValue('t', Integer.toString(Runtime.getRuntime().availableProcessors())));

            if (cli.hasOption('x')) {
                attributes = Document.parse(cli.getOptionValue('x'));
            }

            if (cli.hasOption('f')) {
                path = Paths.get(cli.getOptionValue('f'));
            }
            else {
                path = Paths.get(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("iostat.log")).toURI());
            }

        } catch (ParseException e) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("iostat2mongo", options);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public MongoClient getClient() {
        return client;
    }

    public MongoCollection<Document> getCollection() {
        return collection;
    }

    public MongoDatabase getDatabase() {
        return database;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreads() {
        return threads;
    }

    public List<Path> getFiles() throws IOException {
        if (Files.isDirectory(path)) {
            return Files.walk(path)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toList());
        }
        else {
            return List.of(path);
        }
    }

    public Document getAttributes() {
        return attributes;
    }

    public void cleanup() {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
        }
    }
}
