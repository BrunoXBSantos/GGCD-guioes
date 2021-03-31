package Ex2;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ToParquet {


    // Carregar o Schema
    public static Schema getSchema() throws IOException {
        InputStream is = new FileInputStream("schema.parquet");
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    //converter os dados em texto para parquet. Etapa de map numa tarefa de map-reduce
    // vou ler um fix de texto, long - text
    // quero produzir 1 doc/registo parquet: void(n vou ter reduce) - GenericRe
    public static class ToParquetMapper extends Mapper<LongWritable, Text, Void, GenericRecord>{
        private Schema schema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            schema = getSchema();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // por cada linha crio 1 registo de avro

            if(key.get() == 0){
                String line = value.toString(); // Skip header
            }
            else {

                GenericRecord record = new GenericData.Record(schema);

                String[] line = value.toString().split("\t");

                // 1º elemento its the tconst
                record.put("tconst", line[0]);
                record.put("titleType", line[1]);
                record.put("primaryTitle", line[2]);
                record.put("originalTitle", line[3]);
                record.put("isAdult", line[4]);
                record.put("startYear", (line[5]));
                record.put("endYear", (line[6]));
                record.put("runtimeMinutes", line[7]);

                // preencher os nomes dos generos
                List<String> genres = new ArrayList<>();
                for (String genre : line[8].split(",")) {
                    genres.add(genre);
                }
                record.put("genres", genres);

                context.write(null, record);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // config uma tarefa
        Job job = Job.getInstance(new Configuration(), "Ex2");

        // 1º qual é o jar. Pedir ao sistema que encontre o jar onde estao estas classes
        job.setJarByClass(ToParquet.class);

        // configurar cada uma das classes: map e reduce
        job.setMapperClass(ToParquetMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GenericRecord.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("/home/bruno-santos/Desktop/GGCD/git/GGCD-guioes/Gz/title.basics.tsv.bz2"));

        // config a saida para um fix avro-parquet


        //config o fix de saida
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        //qual é o schema que vamos usar
        AvroParquetOutputFormat.setSchema(job,getSchema());
        FileOutputFormat.setOutputPath(job, new Path("Ex2_output"));


        // executar a tarefa
        job.waitForCompletion(true);
    }
}
