package com.jianqing.netflix;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jianqingsun on 10/31/17.
 * <p>
 * Supposedly we have a Kafka Queue to host movie URL on Wikipedia
 * This is task is used to scrape web page content, build movie metadata and insert movies to database.
 */
public class DataIngestTask implements TaskInterface {

    public Consumer<Long, String> consumer;

    @Override
    public void init() {
        consumer = createConsumer();
    }

    @Override
    public int run() {
        System.out.println("Running data ingestion task ...");

        List<String> urls = pullMsgsMock();
        List<MovieMetadata> movieMetadatas = generateMovieMetaDataFromURL(urls);
        try {
            saveDB(movieMetadatas);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }


    /**
     * Mock pull messages from queue
     *
     * @return
     */
    private List<String> pullMsgsMock() {
        return Arrays.asList("https://en.wikipedia.org/wiki/The_Fate_of_the_Furious",
                "https://en.wikipedia.org/wiki/Beauty_and_the_Beast_(2017_film)",
                "https://en.wikipedia.org/wiki/Despicable_Me_3",
                "https://en.wikipedia.org/wiki/Captain_America:_Civil_War",
                "https://en.wikipedia.org/wiki/Rogue_One");
    }

    /**
     * Pull msgs from queue
     *
     * @return
     */
    private List<String> pullMsgs() {
        List<String> msgs = new ArrayList();
        ConsumerRecords<Long, String> consumerRecords =
                consumer.poll(100);

        if (consumerRecords.count() == 0) {
            System.out.println("There is no message in queue.");
            return msgs;
        }

        for (ConsumerRecord<Long, String> record : consumerRecords) {
            System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                    record.key(), record.value(),
                    record.partition(), record.offset());

            String url = record.value();
            msgs.add(url);

        }

        consumer.commitAsync();
        return msgs;
    }

    /**
     * Save movie metadata to NoSQL database. In demo, we save it in local file system
     *
     * @param movieMetadatas
     */
    private void saveDB(List<MovieMetadata> movieMetadatas) throws IOException {
        File file = new File("/tmp/movie");
        DatumWriter<MovieMetadata> movieDatumWriter = new SpecificDatumWriter<>(MovieMetadata.class);
        DataFileWriter<MovieMetadata> dataFileWriter = new DataFileWriter<>(movieDatumWriter);
        if (movieMetadatas.size() > 0) {
            dataFileWriter.create(movieMetadatas.get(0).getSchema(), file);
            for (MovieMetadata data : movieMetadatas) {
                dataFileWriter.append(data);
            }
            dataFileWriter.close();
        }
    }

    /**
     * Make http call to get content of page and build movie meta data
     *
     * @param urls
     * @return movie metadata
     */
    private List<MovieMetadata> generateMovieMetaDataFromURL(List<String> urls) {
        List<MovieMetadata> movies = new ArrayList<>();
        movies.add(
                MovieMetadata.newBuilder()
                        .setTitle("The Fate of the Furious")
                        .setDirector("F. Gary Gray")
                        .setCharacters(Arrays.asList(
                                Actor.newBuilder().setFirstname("Vin").setLastname("Diesel").build(),
                                Actor.newBuilder().setFirstname("Dwayne").setLastname("Johnson").build())
                        )
                        .setBoxOffice(1290000000)
                        .build());

        movies.add(
                MovieMetadata.newBuilder()
                        .setTitle("Beauty and the Beast")
                        .setDirector("Bill Condon")
                        .setCharacters(Arrays.asList(
                                Actor.newBuilder().setFirstname("Emma").setLastname("Watson").build(),
                                Actor.newBuilder().setFirstname("Dan").setLastname("Stevens").build())
                        )
                        .setBoxOffice(1263000000)
                        .build());
        movies.add(
                MovieMetadata.newBuilder()
                        .setTitle("The Fate of the Furious")
                        .setDirector("F. Gary Gray")
                        .setCharacters(Arrays.asList(
                                Actor.newBuilder().setFirstname("Vin").setLastname("Diesel").build(),
                                Actor.newBuilder().setFirstname("Dwayne").setLastname("Johnson").build())
                        )
                        .setBoxOffice(1290000002)
                        .build());

        movies.add(
                MovieMetadata.newBuilder()
                        .setTitle("The Fate of the Furious")
                        .setDirector("F. Gary Gray")
                        .setCharacters(Arrays.asList(
                                Actor.newBuilder().setFirstname("Vin").setLastname("Diesel").build(),
                                Actor.newBuilder().setFirstname("Dwayne").setLastname("Johnson").build())
                        )
                        .setBoxOffice(1290000003)
                        .build());

        return movies;
    }

    @Override
    public void clean() {
        System.out.println("Cleaning data ingestion task ...");
        if(consumer != null){
            consumer.close();
        }
    }

    private Consumer<Long, String> createConsumer() {
        return null;
    }
}
