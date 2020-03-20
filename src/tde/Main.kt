package basic

import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.Reducer
import tde.AbstractDataMapper
import tde.Item
import tde.UnitWritable
import tde.runJob
import java.io.DataInput
import java.io.DataOutput

/**
 * Pra rodar a versão completa, incluir arquivo transactions.csv na pasta in
 */
fun main(args: Array<String>) {
    runJob<Ex1, Ex1.Mapper1, Ex1.Reducer1, Text, BooleanWritable>(args, "output-1")
    runJob<Ex2, Ex2.Mapper2, Ex2.Reducer2, IntWritable, UnitWritable>(args, "output-2")
    runJob<Ex3, Ex3.Mapper3, Ex3.Reducer3, Text, LongWritable>(args, "output-3")
    runJob<Ex4, Ex4.Mapper4, Ex4.Reducer4, YearMerchandiseKey, LongWritable>(args, "output-4")
    runJob<Ex5, Ex5.Mapper5, Ex5.Reducer5, YearMerchandiseKey, LongWritable>(args, "output-5")
}


object Ex1 {
    class Mapper1 : AbstractDataMapper<Text, BooleanWritable>() {
        //Mapear para true quando o dado é do brasil, ou false caso contrário
        override fun transform(value: Item) = BooleanWritable(value.country == "Brazil")

        //Usar o país como chave
        override fun key(value: Item) = Text(value.country)

    }

    class Reducer1 : Reducer<Text, BooleanWritable, Text, Int>() {
        override fun reduce(key: Text, values: MutableIterable<BooleanWritable>, context: Context) {
            //Escrever em [key] o total de valores em [values] que são "true"
            context.write(key, values.count(BooleanWritable::get))
        }
    }
}
//TODO: Sorting
object Ex2 {
    class Mapper2 : AbstractDataMapper<IntWritable, UnitWritable>() {
        override fun transform(value: Item) = UnitWritable

        override fun key(value: Item) = IntWritable(value.year)

    }

    class Reducer2 : Reducer<IntWritable, UnitWritable, IntWritable, Int>() {
        override fun reduce(key: IntWritable, values: MutableIterable<UnitWritable>, context: Context) {
            context.write(key, values.count())
        }
    }
}

object Ex3 {

    class Mapper3 : AbstractDataMapper<Text, LongWritable>() {
        override fun transform(value: Item) = LongWritable(
                //Caso não seja 2016, não incluir
                when (value.year) {
                    // Usar quantidade caso seja dado de 2016
                    2016 -> value.quantity
                    // Caso contrário, usar 0
                    else -> 0
                }
        )

        override fun key(value: Item) = Text(value.merchandise)
    }

    class Reducer3 : Reducer<Text, LongWritable, Text, LongWritable>() {
        override fun reduce(key: Text, values: MutableIterable<LongWritable>, context: Context) {
            context.write(key, LongWritable(values.sumBy { it.get().toInt() }.toLong()))
        }
    }
}

/**
 * Chave composta de ano e mercadoria.
 * Compatível com hadoop e utilizada no exercícios 4 e 5
 * <br/>
 * Example:
 * ```kotlin
 * val keyA = YearMerchandiseKey(2016, "Meat")
 * val keyB = YearMerchandiseKey(2017, "Meat")
 * val comparison = keyA.compareTo(keyB)
 * ```
 */
class YearMerchandiseKey(
        var year: Int = 0,
        var merchandise: String = ""
) : WritableComparable<YearMerchandiseKey> {
    override fun compareTo(other: YearMerchandiseKey): Int {
        val yearComparison = year.compareTo(other.year)
        if (yearComparison != 0) {
            return yearComparison
        }
        val merchComparison = merchandise.compareTo(other.merchandise)
        if (merchComparison != 0) {
            return merchComparison
        }
        return 0
    }

    override fun readFields(p0: DataInput) {
        year = p0.readInt()
        merchandise = p0.readUTF()
    }

    override fun write(p0: DataOutput) {
        p0.writeInt(year)
        p0.writeUTF(merchandise)
    }

}

object Ex4 {

    class Mapper4 : AbstractDataMapper<YearMerchandiseKey, LongWritable>() {
        override fun transform(value: Item): LongWritable {
            return LongWritable(
                    value.quantity
            )
        }

        override fun key(value: Item) = YearMerchandiseKey(value.year, value.merchandise)
    }

    class Reducer4 : Reducer<YearMerchandiseKey, LongWritable, Text, DoubleWritable>() {
        override fun reduce(key: YearMerchandiseKey, values: MutableIterable<LongWritable>, context: Context) {
            var sum = 0L
            var total = 0L
            for (value in values) {
                sum += value.get()
                total++
            }
            context.write(Text("${key.merchandise} - ${key.year}"), DoubleWritable(sum.toDouble() / total))
        }
    }
}

object Ex5 {


    class Mapper5 : AbstractDataMapper<YearMerchandiseKey, LongWritable>() {
        override fun transform(value: Item): LongWritable {
            return LongWritable(
                    //Igual ao exercício
                    when (value.country) {
                        "Brazil" -> value.quantity
                        else -> 0
                    }
            )
        }

        override fun key(value: Item) = YearMerchandiseKey(value.year, value.merchandise)
    }

    class Reducer5 : Reducer<YearMerchandiseKey, LongWritable, Text, DoubleWritable>() {
        override fun reduce(key: YearMerchandiseKey, values: MutableIterable<LongWritable>, context: Context) {
            var sum = 0L
            var total = 0L
            for (value in values) {
                sum += value.get()
                total++
            }
            context.write(Text("${key.merchandise} - ${key.year}"), DoubleWritable(sum.toDouble() / total))
        }
    }
}