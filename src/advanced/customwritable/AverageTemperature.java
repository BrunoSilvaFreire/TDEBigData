package advanced.customwritable;

import basic.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire-estudante");

        //registro das classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //Definição das saidas
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FireAvgTempWritable.class);

        //arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        /**
         * funcao de map
         * chamada para cada linha e como o resultado vai gerar(chave,value)
         *a chave eh global, então deve existir uma chave unica(media)
         * o valor eh um objeto composto por N e value
         * */

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //obtendo linha
            String linha = value.toString();

            // quebrando em colunas
            String[] colunas = linha.split(",");

            //obtendo o valor da temperatura
            float vlr = Float.parseFloat(colunas[8]);

            //criando o objeto composto
            FireAvgTempWritable val = new FireAvgTempWritable(1,vlr);
            // Float.parseFloat(colunas[8])

            //passando para o reduce no formato <media, obj composto>
            con.write(new Text("Media"),val);


        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            float sumN = 0.0f;
            float sumVlrs = 0.0f;

            for (FireAvgTempWritable obj: values){
                sumN += obj.getN();
                sumVlrs += obj.getValue();
            }


            //escrevendo o resultado final
            con.write(word,new FloatWritable(sumVlrs/sumN));

        }
    }

}
