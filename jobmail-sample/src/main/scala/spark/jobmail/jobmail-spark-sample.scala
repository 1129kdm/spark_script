package spark.jobmail

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class JobMailUser(usr_mgr_no: Int, mail: String, mail_management_id: Int)
case class mySearchCondition(usr_mgr_no: String, knsk_mgr_no: String, kyub_flg: String, tdfk_cd: String, area1_cd: String, area2_cd: String, sarea_cd: String)
case class newJob(job_mgr_no: String, kyubo_sort: String, tdfk_cd: String, area1_cd: String, area2_cd: String, sarea_cd: String, gyo_name: String)

object JobMail {
  ////////////////////
  // メイン処理
  // args(0) csv path 対象者
  // args(1) csv path 仕事条件
  // args(2) csv path 新着案件
  // args(3) output path アウトプット先ディレクトリ
  ////////////////////
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val jobMailUserRDD = sc.textFile(args(0)).filter(line => !line.contains("management_id")).map { line =>
      val elements = line.split(",")
      (elements(0).toInt, elements(1), elements(2).toInt)
    }

    val mySearchConditionRDD = sc.textFile(args(1)).map { line =>
      val elements = line.split(",")
      (elements(0), elements(1), elements(2), elements(3), elements(4), elements(5), elements(6))
    }

    val newJobRDD = sc.textFile(args(2)).map { line =>
      val elements = line.split("\t")
      (elements(0), elements(1), elements(2), elements(3), elements(4), elements(5), elements(6))
    }


    //***** create dataframe
    val jobMailUserDF = jobMailUserRDD.map { case (usr_mgr_no, mail, mail_management_id) =>
      JobMailUser(usr_mgr_no, mail, mail_management_id)
    }.toDF().cache()

    val mySearchConditionDF = mySearchConditionRDD.map { case (usr_mgr_no, knsk_mgr_no, kyub_flg, tdfk_cd, area1_cd, area2_cd, sarea_cd) =>
      mySearchCondition(usr_mgr_no, knsk_mgr_no, kyub_flg, tdfk_cd, area1_cd, area2_cd, sarea_cd)
    }.toDF().cache()

    val newJobDF = newJobRDD.map { case (job_mgr_no, kyubo_sort, tdfk_cd, area1_cd, area2_cd, sarea_cd, gyo_name) =>
      newJob(job_mgr_no, kyubo_sort, tdfk_cd, area1_cd, area2_cd, sarea_cd, gyo_name)
    }.toDF().cache()


    // カラム名が被るのでカラム名の変更
    val mySearchPrefix = "ms_"
    var mySearch = mySearchConditionDF.as("mySearch")
    mySearch.columns.foreach { col => mySearch = mySearch.withColumnRenamed(col, s"${mySearchPrefix}${col}") }

    val joinDataFrame = jobMailUserDF.join(mySearch, jobMailUserDF("usr_mgr_no") === mySearch("ms_usr_mgr_no"), "inner")

    val joinUserCondAndJob = joinDataFrame.join(newJobDF,
      joinDataFrame(s"${mySearchPrefix}tdfk_cd") === newJobDF("tdfk_cd"),
      "inner")

    val joinUserCondAndJobRDD = joinUserCondAndJob.rdd

    val userAndJobRDD = joinUserCondAndJobRDD.map { row =>
      (row.get(0), (row.get(11),List( row.get(1),   // mail
                                      row.get(2),   // mail_management_id
                                      row.get(4),   // ms_knsk_mgr_no
                                      row.get(5),   // ms_kyub_flg
                                      row.get(6),   // ms_tdfk_cd
                                      row.get(7),   // ms_area1_cd
                                      row.get(8),   // ms_area2_cd
                                      row.get(9),   // ms_sarea_cd
                                      row.get(10),  // job_mgr_no
                                      row.get(11),  // kyubo_sort
                                      row.get(13),  // area1_cd
                                      row.get(14),  // area2_cd
                                      row.get(15),  // sarea_cd
                                      row.get(16)   // gyo_name
                                    )
                    )
      )
    }

    // マッチング
    val matchedRDD = userAndJobRDD.filter { data =>
      // area1_cdでfilter
      data._2._2.apply(5) == data._2._2.apply(10) || data._2._2.apply(5) == "null"
    }.filter { data =>
      // area2_cdでfilter
      data._2._2.apply(6) == data._2._2.apply(11) || data._2._2.apply(6) == "null"
    }.filter { data =>
      // sarea_cdでfilter
      data._2._2.apply(7) == data._2._2.apply(12) || data._2._2.apply(7) == "null"
    }

    val sortedRDD = matchedRDD.groupByKey.map { value =>
      (value._1, value._2.toList)
    }.map { value =>
      (value._1, value._2.sortWith(_._1.asInstanceOf[String] > _._1.asInstanceOf[String]))
    }.map { value =>
      (value._1, value._2.map { value2 =>
          List(value2._2.apply(8), value2._2.apply(13))
        }
      )
    }

    // Output用に変換
    val finalyRDD = sortedRDD.map { value =>
      List(value._1, value._2.map { value2 =>
                                    value2.mkString(" ")
                                  }.mkString("\n")
          ).mkString(",")
    }

    // RDDを指定したディレクトリにテキストファイルとして出力する
    finalyRDD.saveAsTextFile(args(3))

    sc.stop()
  }
}
