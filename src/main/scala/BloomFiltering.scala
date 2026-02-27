import org.apache.spark.sql.SparkSession
import scala.io.StdIn
import scala.util.Random
import scala.util.hashing.MurmurHash3
import org.apache.spark.rdd.RDD


//@main
object BloomFiltering {

  def main(args: Array[String]): Unit = {
    println("Program is Running...")

//    user inputs m, p, node.
    println("Provide the expected number of items (m) to be inserted into the Bloom filter:")
    val m = StdIn.readLine().toInt

    println("Provide the maximum allowed false positive probability (p):")
    val p = StdIn.readLine().toDouble

    println("Provide the maximum number of records per partition (node):")
    val node = StdIn.readLine().toInt

// just during working, instead of keep entering the values
//    val m = 20
//    val p = 0.1
//    val node = 2

    println(s"The provided values are: m=$m, p=$p, node=$node")

    // Equations calculation of n, k and rddPartNum
    val n = -math.ceil((m * math.log(p)) / math.pow(math.log(2), 2)).toLong
    // m = 20, p = 0.1 -> n = 95
    val k = math.ceil((n / m) * math.log(2)).toInt
    // n = 95, m = 20 -> k = 3
    val rddPartNum = math.ceil(m.toDouble / node.toDouble).toInt
    // m = 20, node = 2 -> rddPartNum = 10
    println(s"The calculated values are: n=$n, k=$k, partitions=$rddPartNum")

    // task 1: data creation and storage
    // data generation: a unique one
    val data = Stream.continually(Random.nextInt(Int.MaxValue)).distinct.take(m)

    // creating the spark session
    val spark = SparkSession
      .builder()
      .appName("SearchEngine")
      .master("local[*]")
      .config("spark.some.config.option", "")
      .getOrCreate()
    // spark context
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")


    // storing the values in the RDD in parallel
    val rddData = sc.parallelize(data, rddPartNum)
    // parallelize is fine here since the data is already local still...
    println("The data is stored in parallel in an RDD")

    // task 2: bloom filtering
    // Represent the bit array as an RDD of bits
    // in this case using .range() is more scalable for the memory than parallelize, as parallelize may lead to "out of memory" in large data case.
    val rddDataBits = sc.range(0L,n,1L, rddPartNum).map(i => (i, false)) // 0L and 1L since the n is long, the program may fail otherwise.

    // creating the hash functions for long integers
    def hashFunctions(item: Long, k: Int, n: Long): Seq[Long] = {
      val h1 = MurmurHash3.bytesHash(BigInt(item).toByteArray)
      val h2 = MurmurHash3.bytesHash(BigInt(item + 1).toByteArray)
      (0 until k).map(i => ((h1.toLong + i.toLong * h2.toLong) & 0x7fffffffffffffffL) % n)
    }
    // hashing the rdd data values, each one is hashed k times
//    faltMap in this case returns k of each item
//    so each value fills the hashed position to true (tha value exists)
    val hashedValues: RDD[(Long, Boolean)] = rddData.flatMap { item =>
      hashFunctions(item, k, n).map(bitIndex => (bitIndex, true))
    }

// saving the values into a rdd bloom filter where each value has a (true | false) whether it  exists or does not.
    // this is the final rdd, in which we will later search for values in.
    val bloomFilterRDD : RDD[(Long, Boolean)] = rddDataBits
      .union(hashedValues) // all the k count of hashed values for each number in the rddDatabits.
      .reduceByKey(_ || _) // in case ( true||false) so we can decide which to store.
    println("Bloom filter is ready to be looked up into")


    // task 3: lookup
    println("Provide the word you are looking for:")
    val input =  data.head
    // hashing the input
    val bitIndices = hashFunctions(input, k, n)
    // checking weather it exists in the rdd bloom filter
    val bits = bloomFilterRDD.filter{ case (index, _) => bitIndices.contains(index)}.map(_._2).collect()

    println(s"Checking item: $input")
    println(s"Hash indices: ${bitIndices.mkString(", ")}")
    println(s"Bits values at those indices: ${bits.mkString(", ")}")
    println(s"Item may exist in Bloom filter? ${bits.forall(identity)}")

    // Task 4: Handling Capacity Overflow
    // sol1: The cost is low accuracy depends on the data size
    // where then we can lower the hash functions count (k) to maybe k-1 or k=1 depends on the m count
    // then we store on top of the ones that already stored.
    // in this case the rate of false pos is a bit high.
    println("Approach 1: Handling Capacity with lower k values - storing on top")
    // first 10 exiist, last ones does not.
    val testData = data.take(10).toArray ++ Array(0,1,2,3,4,5,6,7,8,9)
    println(s"test data is: ${testData.mkString("Array(", ", ", ")")}")
    val tempK =  math.max(k - 1, 1)
    val inputDataSize = testData.length
    println(s"Inserting additional $inputDataSize items with k=$tempK")
    println(s"Testing ${testData.length} items — first 10 should exist, last 10 should not.")

    // hashing the values
    val overflowHashedValues = sc.parallelize(testData.takeRight(10), rddPartNum).flatMap { item =>
      hashFunctions(item, tempK, n).map(bitIndex => (bitIndex, true))
    }

    val updatedBloomFilterRDD = bloomFilterRDD
      .union(overflowHashedValues)
      .reduceByKey(_ || _)

    def checkItem(item: Int): Boolean = {
      val ind = hashFunctions(item, tempK, n)
      val bits = updatedBloomFilterRDD
        .filter { case (index, _) => ind.contains(index) }
        .map(_._2)
        .collect()
      bits.forall(identity)
    }

    // evaluation - first 10 guranteed to exist in the data, the last 10 are not.
    val trueItems = testData.take(10)
    val falseItems = testData.takeRight(10)

    val truePositives = trueItems.count(checkItem)
    val falsePositives = falseItems.count(checkItem)

    val truePositiveRate = truePositives.toDouble / trueItems.length
    val falsePositiveRate = falsePositives.toDouble / falseItems.length

    println(s"True Positive Rate: ${(truePositiveRate * 100).formatted("%.2f")} %")
    println(s"False Positive Rate: ${(falsePositiveRate * 100).formatted("%.2f")} %")


    // sol2: An additional bloom filter layer on the side.
    val overflowData = testData.takeRight(10)
    println(s"Inserting ${overflowData.length} overflow items into a new Bloom filter layer.")

    // additional bloom filter layer
    val rddDataBits2 = sc.range(0L, n, 1L, rddPartNum).map(i => (i, false))

    val hashedValues2 = sc.parallelize(overflowData, rddPartNum).flatMap { item =>
      hashFunctions(item, k, n).map(bitIndex => (bitIndex, true))
    }

    val bloomFilterLayer2 = rddDataBits2.union(hashedValues2).reduceByKey(_ || _)

    // Combined lookup: check both layers
    def checkInLayers2(item: Int): Boolean = {
      val bitIndices = hashFunctions(item, k, n)
      val bits1 = bloomFilterRDD.filter { case (index, _) => bitIndices.contains(index) }.map(_._2).collect()
      val bits2 = bloomFilterLayer2.filter { case (index, _) => bitIndices.contains(index) }.map(_._2).collect()

      bits1.forall(identity) || bits2.forall(identity)
    }

    // evalutaion
    val trueItems2 = testData.take(10)
    val falseItems2 = testData.takeRight(10)

    val truePositives2 = trueItems2.count(checkInLayers2)
    val falsePositives2 = falseItems2.count(checkInLayers2)

    val truePositiveRate2 = truePositives2.toDouble / trueItems2.length
    val falsePositiveRate2 = falsePositives2.toDouble / falseItems2.length

    println(s"True Positive Rate (Layered): ${(truePositiveRate2 * 100).formatted("%.2f")} %")
    println(s"False Positive Rate (Layered): ${(falsePositiveRate2 * 100).formatted("%.2f")} %")

  }
}


