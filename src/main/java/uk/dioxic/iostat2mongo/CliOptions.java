package uk.dioxic.iostat2mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import lombok.Getter;
import org.apache.commons.cli.*;
import org.bson.Document;
import reactor.core.Exceptions;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class CliOptions {

    private MongoClient client;
    private MongoCollection<Document> collection;
    private MongoDatabase database;
    private int batchSize;
    private int threads;
    private Path path;
    private Path logFile;
    private Document attributes;
    private String username;
    private String password;
    private String authenticationDatabase;
    private List<String> filters;

    private BufferedWriter bw;

    public CliOptions(String[] args) {
        Options options = new Options();
        options.addRequiredOption("f", "path", true, "iostat path path");
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
        options.addOption("P", "poolSize", true, "connection pool size (default: 100)");
        options.addOption("F", "filters", true, "comma-delimited list of metrics to include (default: all)");
        options.addOption("l","log", true, "log file path");

        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cli = parser.parse(options, args);

            MongoClientSettings.Builder mongoClientBuilder = MongoClientSettings.builder();
            mongoClientBuilder.applicationName("iostat loader");
            mongoClientBuilder.applyToConnectionPoolSettings(builder -> builder.maxSize(Integer.parseInt(cli.getOptionValue('P', "100"))));
//            mongoClientBuilder.applyToSocketSettings(soc -> soc.readTimeout());

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

            client = MongoClients.create(mongoClientBuilder.build());

            database = client.getDatabase(cli.getOptionValue('d', "test"));
            collection = database.getCollection(cli.getOptionValue('c', "iostats"));
            batchSize = Integer.parseInt(cli.getOptionValue('b', "1000"));
            threads = Integer.parseInt(cli.getOptionValue('t', "1"));
            filters = List.of(cli.getOptionValue('F', "").split("\\s*,\\s*"));
            attributes = cli.hasOption('x') ? Document.parse(cli.getOptionValue('x')) : new Document();

            if (cli.hasOption('x'))
                attributes = Document.parse(cli.getOptionValue('x'));

            if (cli.hasOption('f'))
                path = Paths.get(cli.getOptionValue('f'));

            if (cli.hasOption('l'))
                logFile = Paths.get(cli.getOptionValue('l'));

        } catch (ParseException e) {
            HelpFormatter help = new HelpFormatter();
            help.printHelp("iostat2mongo", options);
        }
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

    public void log(Document doc) {
        if (logFile != null)
            try {
                if (bw == null) {
                    bw = Files.newBufferedWriter(logFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                }
                bw.write(DocumentUtil.toJson(doc) + "\n");
            }
            catch (IOException e){
                throw Exceptions.propagate(e);
            }
    }

    public void cleanup() {
        client.close();
        if (bw != null) {
            try {
                bw.close();
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }
    }

    @Override
    public String toString() {
        return "CliOptions{" +
                "collection=" + collection.getNamespace() +
                ", database=" + database.getName() +
                ", batchSize=" + batchSize +
                ", threads=" + threads +
                ", path=" + path +
                ", logFile=" + logFile +
                ", attributes=" + attributes +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", authenticationDatabase='" + authenticationDatabase + '\'' +
                ", filters=" + filters +
                '}';
    }
}
