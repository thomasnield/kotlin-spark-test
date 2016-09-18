
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import java.io.Serializable

fun main(args: Array<String>) {

    val conf = SparkConf()
            .setMaster("local")
            .setAppName("Line Counter")

    val sc = JavaSparkContext(conf)

    class MyItem(val id: Int, val value: String): Serializable

    val input = sc.parallelize(listOf(MyItem(1,"Alpha"),MyItem(2,"Beta")))

    val letters = input.flatMap { it.value.split(Regex("(?<=.)")) }
            .map { it.toUpperCase() }
            .filter { it.matches(Regex("[A-Z]")) }

    println(letters.collect())
}