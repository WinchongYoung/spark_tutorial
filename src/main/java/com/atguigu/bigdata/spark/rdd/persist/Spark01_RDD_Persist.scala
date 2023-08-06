package com.atguigu.bigdata.spark.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {

    def main(args: Array[String]): Unit = {


        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)

        val lines = sc.makeRDD(
            List("Hadoop Hive Hbase", "Spark scala Java")
        )
        val words = lines.flatMap(_.split(" "))
        val wordToOne = words.map(
            t => {
                println("*************************")
                (t, 1)
            }
        )
        // 设定数据持久化
        // cache方法可以将血缘关系进行修改，添加一个和缓存相关的依赖关系
        // cache默认持久化级别为 MEMORY_ONLY 所以cache操作不安全。
        // 注意  使用了wordToOne.cache() 就不能使用persist方法
        // 否则会抛出异常 Cannot change storage level of an RDD after it was already assigned a level
        // wordToOne.cache()
        // 如果持久化的话，那么持久化的文件只能自己用。而且使用完毕后， 会删除
        wordToOne.persist(StorageLevel.MEMORY_AND_DISK)

        val wordToCount = wordToOne.reduceByKey(_+_)
        println(wordToCount.toDebugString)
        wordToCount.collect()//.foreach(println)
        println("--------------------------------------------")
        val rdd2: RDD[(Int, Iterable[(String, Int)])] = wordToOne.groupBy(_._2)
        rdd2.collect()
        println(wordToCount.toDebugString)

        sc.stop()

    }
}
