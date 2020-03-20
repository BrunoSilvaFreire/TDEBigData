package tde

import basic.Ex1
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.log4j.BasicConfigurator
import java.io.DataInput
import java.io.DataOutput
import java.io.File

enum class Flux {
    IMPORT,
    EXPORT
}

data class Item(
        val country: String,
        val year: Int,
        val code: Int,
        val merchandise: String,
        val flux: Flux,
        val value: Double,
        val weight: Double,
        val unit: String,
        val quantity: Long,
        val category: String
)

object UnitWritable : Writable {
    override fun readFields(p0: DataInput?) {

    }

    override fun write(p0: DataOutput?) {
    }

}

abstract class AbstractDataMapper<K, T> : Mapper<LongWritable, Text, K, T>() where T : Writable {
    abstract fun transform(value: Item): T
    abstract fun key(value: Item): K
    override fun map(key: LongWritable, value: Text, context: Context) {
        try {
            val (
                    countryStr, yearStr, codeStr, merchStr,
                    fluxStr, valueStr, weightStr, unitStr,
                    quantityStr, categoryStr
            ) = value.toString().split(';')
            val flux = when (fluxStr) {
                "Import" -> Flux.IMPORT
                "Export" -> Flux.EXPORT
                else -> return
            }
            val item = Item(
                    countryStr,
                    yearStr.toIntOrNull() ?: 0,
                    codeStr.toIntOrNull() ?: 0,
                    merchStr,
                    flux,
                    valueStr.toDouble(),
                    weightStr.toDoubleOrNull() ?: 0.0,
                    unitStr,
                    quantityStr.toLongOrNull() ?: 0,
                    categoryStr
            )
            context.write(key(item), transform(item))
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

}

inline fun <reified J, reified M, reified R, reified K, reified V> runJob(args: Array<String>, outputName: String): Boolean
        where M : Mapper<*, *, K, V>,
              R : Reducer<K, V, *, *> {
    BasicConfigurator.configure()
    val c = Configuration()
    val files = GenericOptionsParser(c, args).remainingArgs
    // arquivo de entrada
    val input = Path(files[0])

    // arquivo de saida
    val outFinal = "${files[1]}/$outputName"
    println("Final @ $outFinal")
    val output = Path(outFinal)
    val outFolder = File(outFinal)
    with(outFolder) {
        if (exists()) {
            println("Deleting ${outFolder.absolutePath}")
            deleteRecursively()
        }
    }
    // criacao do job e seu nome
    val j = Job(c, "wordcount-professor")
    j.setJarByClass(J::class.java)
    j.mapperClass = M::class.java
    j.reducerClass = R::class.java
    j.outputKeyClass = K::class.java
    j.outputValueClass = V::class.java
    FileInputFormat.addInputPath(j, input)
    println(output)
    FileOutputFormat.setOutputPath(j, output)
    // lanca o job e aguarda sua execucao
    return j.waitForCompletion(false)
}