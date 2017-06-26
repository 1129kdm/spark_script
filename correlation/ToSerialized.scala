package org.apache.spark

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.io.Writable

import org.apache.spark._
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, MatrixEntry, CoordinateMatrix}


class ToSerialized extends java.io.Serializable {
    def settingOfPutToPearsonTable(pearsonColumnFamilyName:String,row:String, column:String, value:String) = {
        var p = new Put(Bytes.toBytes(row))
        p.add(Bytes.toBytes(pearsonColumnFamilyName), Bytes.toBytes(column), Bytes.toBytes(value))
        (new ImmutableBytesWritable, p)
    }

    // スコアを計算して順番を入れ替えて返す
    def calcScore(sc:org.apache.spark.SparkContext ,tableName:String, itemList: List[String], correlMatrix:org.apache.spark.mllib.linalg.Matrix) = {
        val hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.set( "hbase.zookeeper.quorum", "dune-nspark-master" )
        hbaseConfig.set( "hbase.zookeeper.property.clientPort", "2181" )
        hbaseConfig.addResource(new Path("/etc/hbase/conf/core-site.xml"))
        hbaseConfig.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
        hbaseConfig.set(TableInputFormat.INPUT_TABLE, tableName)
        hbaseConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
        hbaseConfig.setClass("mapreduce.outputformat.class", classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[String]], classOf[OutputFormat[Object, Writable]])

        // 対象のカラムファミリーをセット
        val columnFamilyName = "evaluation"
        hbaseConfig.set(TableInputFormat.SCAN_COLUMN_FAMILY, columnFamilyName)

        println("hbasデータ取得")
        val hBaseRDD = sc.newAPIHadoopRDD(hbaseConfig,classOf[TableInputFormat],
                                                      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                                                      classOf[org.apache.hadoop.hbase.client.Result]
                                          )

        import scala.collection.JavaConverters._
        val outPut3 = hBaseRDD.map(x => x._2).map(_.list).flatMap(x =>  x.asScala.map(cell =>
                    (
                            Bytes.toStringBinary(CellUtil.cloneRow(cell)),
                            Bytes.toStringBinary(CellUtil.cloneValue(cell)),
                            Bytes.toStringBinary(CellUtil.cloneQualifier(cell))
                        )
                    )
                ).map(x => (x._1, (x._3, x._2.toDouble))).groupByKey.map(x => (x._1, x._2.toArray)).map(y => (y._1, y._2.sorted)).map(z => (z._1, z._2.map(k => k._2))).persist()

        println(s"評価値マトリックス生成")
        val evaluationDenseMatrix = outPut3.map(x => {
                (x._1, new DenseMatrix(x._2.size.toInt, 1, x._2))
            })

        println(s"スコア算出")
        val scoreDenseMatrix = evaluationDenseMatrix.map(x => {
                (x._1, correlMatrix.multiply(x._2))
            })

        println(s"スコア順にしてitem_idとくっつける")
        val scoreRDD = scoreDenseMatrix.map(x => {
                (x._1, itemList.zip(x._2.values).sortWith((a,b) => a._2 > b._2).map(x => "%s:%s".format(x._1, x._2)).take(100))
            })

        println(s"スコアをhbaseに格納")
        var insertDataScore = scoreRDD.map(x => {
                var p = new Put(Bytes.toBytes(x._1))
                var loopNum = x._2.size.toInt
                for( i <- 0 until loopNum) {
                    p.add(Bytes.toBytes("score"), Bytes.toBytes("score" + i), Bytes.toBytes(x._2(i)))
                }
                (new ImmutableBytesWritable, p)
            })

        insertDataScore.saveAsNewAPIHadoopDataset(hbaseConfig)
    }
}
