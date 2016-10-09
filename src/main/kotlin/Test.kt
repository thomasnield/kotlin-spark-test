
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext

fun main(args: Array<String>) {

    class MyItem(val id: Int, val value: String)

    val conf = SparkConf()
            .setMaster("local")
            .setAppName("Line Counter")

    conf.registerKryoClasses(arrayOf(MyItem::class.java))

    val sc = JavaSparkContext(conf)

    val input = sc.parallelize(listOf(MyItem(1,"Alpha"),MyItem(2,"Beta")))

    val letters = input.flatMap { it.value.split(Regex("(?<=.)")) }
            .map { it.toUpperCase() }
            .filter { it.matches(Regex("[A-Z]")) }

    println(letters.collect())
}
