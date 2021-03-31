package Main;

import MapReduce.Ex2;
import MapReduce.Ex4;
import MapReduce.Ex5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Ex5_Main {

    /*
    *   job1, fazer map-reduce para dar as listas de filme-rating
    *   job2, map para escolher apenas os filmes com rating > 9 e colocar o rating na chave para ordenar
    *         e depois fazer a reduce task para voltar ao normal
    * */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // config uma tarefa
        Job job1 = Job.getInstance(new Configuration(), "list_movie_rating");

        // 1º qual é o jar. Pedir ao sistema que encontre o jar onde estao estas classes
        job1.setJarByClass(Ex4.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path("/home/bruno-santos/Desktop/GGCD/guioes/mini/title.basics.tsv"),
                TextInputFormat.class, Ex4.Ex4LeftMapper.class);
        MultipleInputs.addInputPath(job1, new Path("/home/bruno-santos/Desktop/GGCD/guioes/mini/title.ratings.tsv"),
                TextInputFormat.class, Ex4.Ex4RightMapper.class);
        job1.setReducerClass(Ex4.Ex4Reduce.class);

        //config o fix de saida
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path("/home/bruno-santos/Desktop/GGCD/guioes/lab1/Ex5_output/list_movie_rating"));

        //config os tipos de dados comunicados do mapa para o reduce
        // maybe not necessary
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // executar a tarefa
       job1.waitForCompletion(true);



        // JOB 2
        // config uma tarefa
        Job job2 = Job.getInstance(new Configuration(), "rating_least_9");

        // 1º qual é o jar. Pedir ao sistema que encontre o jar onde estao estas classes
        job2.setJarByClass(Ex5.class);

        // configurar cada uma das classes: map e reduce
        job2.setMapperClass(Ex5.RatingLeast9Mapper.class);
        job2.setReducerClass(Ex5.Ex5ReduceTask.class);

        // classe que ordena de forma decrescente
        job2.setSortComparatorClass(Ex5.sortComparator.class);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job2, new Path("/home/bruno-santos/Desktop/GGCD/guioes/lab1/Ex5_output/list_movie_rating"));

        //config o fix de saida
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2, new Path("/home/bruno-santos/Desktop/GGCD/guioes/lab1/Ex5_output/sortDesc_movie_rating"));

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //config os tipos de dados comunicados do mapa para o reduce
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);

        // executar a tarefa
        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }

}
