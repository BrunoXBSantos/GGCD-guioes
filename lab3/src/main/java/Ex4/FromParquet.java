package Ex4;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FromParquet {


    public static Schema getSchemaGenres() throws IOException {
        InputStream is = new FileInputStream("schemaGenres.parquet");
        String ps = new String(is.readAllBytes());
        MessageType mt = MessageTypeParser.parseMessageType(ps);
        return new AvroSchemaConverter().convert(mt);
    }

    // recebe o fix em formato parquet e processo o map-reduce
    // recebe dados em parquet
    // void - generic
    // text(genero)- num files
    // devido ao esquema, vai ler tudo
    public static class FromParquetMapper extends Mapper<Void, GenericRecord, Text, LongWritable> {

        @Override
        protected void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
            GenericData.Array<String> list = (GenericData.Array<String>)value.get("genres");

            for(String genre: list){
                context.write(new Text(genre), new LongWritable(1));
            }
        }
    }

    public static class FromParquetReduce extends Reducer<Text, LongWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for(LongWritable value: values){
                total += value.get();
            }

            context.write(key, new Text(Long.toString(total)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // config uma tarefa
        Job job = Job.getInstance(new Configuration(), "FromParquet_Ex4");

        // 1º qual é o jar. Pedir ao sistema que encontre o jar onde estao estas classes
        job.setJarByClass(FromParquet.class);

        // configurar cada uma das classes: map e reduce
        job.setMapperClass(FromParquetMapper.class);
        job.setReducerClass(FromParquetReduce.class);

        // entre map e reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        // entrada no map
        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path("Ex2_output"));

        // config adicional
        // este schema é igual ao que escrevi para escrever no outro
        // tenho que fazer o esquema e ler com uma funcao, igual a anteriormente
        // mas agora neste, elimino todas as colunas que nao me interessa
        // e para ler o fix, leio so com este schema
        // assim todas as colunas sao ignoradas, as restantes
        // com isto o schema total inicial nao é carregado todo na map
        AvroParquetInputFormat.setRequestedProjection(job, getSchemaGenres());



        //config o fix de saida
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("Ex4_output"));

        // executar a tarefa
        job.waitForCompletion(true);
    }
}