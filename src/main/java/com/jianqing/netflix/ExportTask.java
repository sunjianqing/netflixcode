package com.jianqing.netflix;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


/**
 * Created by jianqingsun on 10/31/17.
 * <p>
 * Export Data to HDFS.
 * For demo, I export data to local with json format
 */
public class ExportTask implements TaskInterface {

    @Override
    public void init() {

    }

    @Override
    public int run() {
        System.out.println("Running export task ...");
        File avroData = new File("/tmp/movie");
        String newLine = System.getProperty("line.separator");

        DatumReader<MovieMetadata> movieDatumReader = new SpecificDatumReader<>(MovieMetadata.class);
        DataFileReader<MovieMetadata> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<>(avroData, movieDatumReader);
            MovieMetadata movie;

            FileWriter fw = new FileWriter("/tmp/moviejson");

            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                movie = dataFileReader.next();
                fw.write(movie.toString() + newLine);
            }
            fw.close();
            dataFileReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


        return 0;
    }

    @Override
    public void clean() {
        System.out.println("Cleaning export task ...");
    }

}
