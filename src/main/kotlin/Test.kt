
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

fun main(args: Array<String>) {

    class MyItem(val id: Int, val value: String)

    val conf = SparkConf()
            .setMaster("local")
            .setAppName("Line Counter")

    conf.registerKryoClasses(MyItem::class)

    val sc = JavaSparkContext(conf)

    val input = sc.parallelize(listOf(MyItem(1,"Alpha"),MyItem(2,"Beta")))

    val letters = input.flatMap { it.value.split(Regex("(?<=.)")) }
            .map { it.toUpperCase() }
            .filter { it.matches(Regex("[A-Z]")) }

    println(letters.collect())
}

//extension function to register Kotlin classes
fun SparkConf.registerKryoClasses(vararg args: KClass<*>) = registerKryoClasses(args.map { it.java }.toTypedArray()
