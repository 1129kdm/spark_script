package org.apache.spark


import org.apache.spark.ToSerialized

import org.apache.hadoop.hbase.client.{Result, Put, HTable, ConnectionFactory}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

import org.apache.spark._
import org.apache.spark.mllib.stat._
import org.apache.spark.mllib.stat.correlation._
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseMatrix}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, MatrixEntry, CoordinateMatrix}
import scala.collection.JavaConverters._



object CalculateScore {
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf())

        // HDFSからアイテムIDマスタCSVを取得
        println("マスタCSV取得")
        val columnMasterRDD=sc.textFile(args(1))
        val hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.set( "hbase.zookeeper.quorum", "dune-nspark-master" )
        hbaseConfig.set( "hbase.zookeeper.property.clientPort", "2181" )
        hbaseConfig.addResource(new Path("/etc/hbase/conf/core-site.xml"))
        hbaseConfig.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

        // 対象のテーブルをセット
        val tableName = args(0)
        hbaseConfig.set(TableInputFormat.INPUT_TABLE, tableName)
        // 対象のカラムファミリーをセット
        val columnFamilyName = "evaluation"
        hbaseConfig.set(TableInputFormat.SCAN_COLUMN_FAMILY, columnFamilyName)
        //**********

        // hbaseからデータ取得
        println("hbasデータ取得")
        val hBaseRDD = sc.newAPIHadoopRDD(hbaseConfig,classOf[TableInputFormat],
                                                      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                                      classOf[org.apache.hadoop.hbase.client.Result]
                                          )


        /**************************
         * First step item_idの0埋め
         **************************/ 
        // rowKeyRDD: org.apache.spark.rdd.RDD[String]
        // 行キーのRDDを生成
        println("行キーRDD生成")
        val rowKeyRDD = hBaseRDD.map(x => Bytes.toString(x._1.get))

        // idPairRDD: org.apache.spark.rdd.RDD[(String, String)]
        // ユーザIDとitem_id(マスタ)がペアのRDDを生成
        println("user_id&item_idのRDD生成")
        val idPairRDD = rowKeyRDD.cartesian(columnMasterRDD)

        // evaluationRDD: org.apache.spark.rdd.RDD[(String, String)]
        // hbaseから評価値の一覧を取得。結果は(user_id, item_id)のタプル
        println("hbaseから取得したデータを変換")
        val evaluationRDD = hBaseRDD.map(x => x._2).map(_.list).flatMap(x =>  x.asScala.map(cell =>
            (
                    Bytes.toStringBinary(CellUtil.cloneRow(cell)),
                    Bytes.toStringBinary(CellUtil.cloneQualifier(cell))
                )
            )
        )

        // subtractRDD: org.apache.spark.rdd.RDD[(String, String)]
        // マスターとhbaseの取得結果の差集合RDDの生成
        println("マスターとhbaseデータの差集合RDD生成")
        val subtractRDD = idPairRDD.subtract(evaluationRDD)

        if (subtractRDD.count > 0) {
            // 評価項目テーブルの穴あきデータを0埋めする
            val jobConf = new JobConf(hbaseConfig,this.getClass)
            jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
            jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
            val insertData = subtractRDD.map(data => {
                  val serialClass2 = new ToSerialized()
                  serialClass2.settingOfPutToPearsonTable("evaluation",data._1,data._2,"0")
              })
            println("0埋め処理実行")
            insertData.saveAsHadoopDataset(jobConf)
        }

        // filledZeroEvaluationRDD: org.apache.spark.rdd.RDD[(String, (String, Double))]
        // user_idと(item_id, value)がキーバリューのRDD生成(0埋め後)
        println("0埋め後のデータ取得")
        val filledZeroEvaluationRDD = hBaseRDD.map(x => x._2).map(_.list).flatMap(x =>  x.asScala.map(cell =>
            (
                    Bytes.toStringBinary(CellUtil.cloneRow(cell)),
                    Bytes.toStringBinary(CellUtil.cloneValue(cell))
                )
            )
        ).map(x => (x._1, x._2.toDouble))

        // 引数からパーティション数を取得
        println("パーティション分割")
        val partitionNum = args(2).toInt

        // itemValueRDD: org.apache.spark.rdd.RDD[(String, Array[Double])]
        // user_idごとにvalueのRDDを作る(user_id,Array[Double])
        println("user_idごとに評価値の配列を作る")
        val itemValueRDD = filledZeroEvaluationRDD.groupByKey.map(x => (x._1, x._2.toArray)).partitionBy(new HashPartitioner(partitionNum)).sortByKey(true)

        // 評価値のベクトルを生成
        println("評価値の配列をベクトルに変換")
        val itemValueVector = itemValueRDD.map(data =>{
            Vectors.sparse(data._2.length, data._2.zipWithIndex.map(e => (e._2, e._1)).filter(_._2 != 0.0))
        })

        // 相関係数算出
        println("相関係数算出")
        val correlMatrix = Statistics.corr(itemValueVector, "pearson")

        // item_idのリスト生成
        val itemList = columnMasterRDD.collect.toList

        // スコアの計算
        println(s"スコア計算開始")
        val scoreTableName = args(0)
        val serialClass3 = new ToSerialized()
        serialClass3.calcScore(sc, scoreTableName, itemList, correlMatrix)

        sc.stop()
    }
}
