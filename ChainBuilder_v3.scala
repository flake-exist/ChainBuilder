import org.apache.spark._
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext._
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession

import CONSTANTS._

object ChainBuilder_v3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._

    val optionsMap  = argsPars(args, usage) //Parse input arguments from command line

    val validMap = argsValid(optionsMap) // Validation input arguments types and building Map

//    val date_tail:String = "2019-07-01"
//    val date_tailPure:Long = DateStrToUnix(date_tail)
    val attrWindowMode:Boolean = true

    //Safe class contains correct input argument types
    val arg_value = ArgValue(
      validMap("date_start").asInstanceOf[String],
      validMap("date_tHOLD").asInstanceOf[String],
      validMap("date_finish").asInstanceOf[String],
      validMap("product_name").asInstanceOf[String],
      validMap("projectID").asInstanceOf[Long],
      validMap("target_numbers").asInstanceOf[Array[Long]],
      validMap("source_platform").asInstanceOf[Array[String]],
      validMap("flat_path").asInstanceOf[Array[String]],
      validMap("output_path").asInstanceOf[String]
    )

    val date_tHOLD:Long = DateStrToUnix(arg_value.date_tHOLD)

    //Create unique name (hashname) for output file basing on next parameters
    val file_HashName = Seq(
      arg_value.projectID.toString,
      arg_value.date_start,
      arg_value.date_finish,
      arg_value.product_name,
      arg_value.target_numbers.mkString(";")).mkString("_")

    //Create path where file will be saved
    val output_path = arg_value.output_path + file_HashName

    // Seq with date_start(Unix Hit) & date_finish(Unix)
    val date_range:Vector[Long] = Vector(
      arg_value.date_start,
      arg_value.date_finish).map(DateStrToUnix)

    //Check `date_finish` is greater than `date_start`
    val date_pure = date_range match {
      case Vector(a,b) if a < b  => date_range
      case Vector(a,b) if a >= b => throw new Exception("`date_start` is greater than `date_finish`")
      case _                     => throw new Exception("`date_range` must have type Vector[Long]")
    }

    /*
    `target_numbers` are Google Analytics goal IDs we want to explore for chain creation and further Attribution.
    For the sake to create chain need to add user non-conversion actions out of site or non-conversion actions on site
    */
    val target_goals = arg_value.target_numbers :+ TRANSIT_ACTION //`TRANSIT_ACTION ` defined in `CONSTANTS` module

    //REGISTRATION
    val path_creator_udf    = spark.udf.register("path_creator",pathCreator(_:Seq[String],_:String):Array[String])
    val channel_creator_udf = spark.udf.register("channel_creator",channel_creator(_:String,_:String,_:String,_:String,_:String,_:String):String)
    val searchInception_udf = spark.udf.register("searchInception",searchInception(_:Seq[Map[String,Long]],_:Long,_:String):Seq[Map[String,Long]])
    val htsTube_udf         = spark.udf.register("htsTube",htsTube(_:Seq[Map[String,Long]]):Seq[String])
    val channelTube_udf     = spark.udf.register("channelTube",channelTube(_:Seq[Map[String,Long]]):Seq[String])
    val extract_value_udf   = spark.udf.register("extractValue",extractValue(_:Seq[Map[String,Long]]):Seq[String])


    //REGISTRATION


    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(arg_value.flat_path:_*)

//    val data = spark.read.option("mergeSchema", "true").parquet(arg_value.flat_path:_*)

    //Select significant matrics for further chain creation
    val data_work = data.select(
      $"ProjectID".cast(sql.types.LongType),
      $"ClientID".cast(sql.types.StringType),
      $"HitTimeStamp".cast(sql.types.LongType),
      $"ga_sourcemedium".cast(sql.types.StringType),
      $"utm_source".cast(sql.types.StringType),
      $"utm_medium".cast(sql.types.StringType),
      $"adr_typenum".cast(sql.types.StringType),
      $"adr_profile".cast(sql.types.StringType),
      $"ga_location".cast(sql.types.StringType),
      $"goal".cast(sql.types.LongType),
      $"src".cast(sql.types.StringType)
    )

    //Customize data by client (`ProjectID`) and date range (`date_start` - `date_finish`)
    val data_custom_0 = data_work.
      filter($"HitTimeStamp" >= date_tHOLD && $"HitTimeStamp" < date_pure(1)).
      filter($"ProjectID"    === arg_value.projectID)



    //Customize data by source (`source_platform`) and current product (`product_name`) if exists
    val data_custom_1 = arg_value.product_name match {
      case product if isEmpty(product) => data_custom_0.filter($"src".isin(arg_value.source_platform:_*))
      case product                     => data_custom_0.filter($"src".isin(arg_value.source_platform:_*)).
                                          filter($"ga_location" === arg_value.product_name)
    }

    /*
    Filter by `target_goals`
    Sort by `ClientID` and `HitTimeStamp` (ascending) to build in future user(`ClientID`) path (chronological sequence of touchpoints(channels)
    */
    val data_custom_2 = data_custom_1.
      withColumn("goal",when($"goal".isNull,TRANSIT_ACTION).otherwise($"goal")).
      filter($"goal".isin(target_goals:_*)).
      sort($"ClientID", $"HitTimeStamp".asc).cache()


    //Find ClientIDs committed conversions
    val actorsID = data_custom_2.
      filter($"goal".isin(arg_value.target_numbers:_*)).
      filter($"HitTimeStamp" >= date_pure(0) && $"HitTimeStamp" < date_pure(1)).
      select($"ClientID").distinct.collect().map(_(0).toString)

    //Filter only relevant data to `actorsID`
    val data_actors = data_custom_2.
      filter($"ClientID".isin(actorsID:_*))


    //Create metric `channel` . It is indivisible unit for future  paths(chains)
    val data_preprocess_0 = data_actors.withColumn("channel", channel_creator_udf(
      $"src",
      $"ga_sourcemedium",
      $"utm_source",
      $"utm_medium",
      $"adr_typenum",
      $"adr_profile")).select(
      $"ClientID",
      $"HitTimeStamp",
      $"goal",
      $"channel"
    )

    //Create new metric `conversion`. Was conversion or not
    val data_preprocess_1 = data_preprocess_0.
      withColumn("conversion",when($"goal" === TRANSIT_ACTION,NO_CONVERSION_SYMBOL).otherwise(CONVERSION_SYMBOL)).select(
      $"ClientID",
      $"channel",
      $"conversion",
      $"HitTimeStamp"
    )

    /* Create metric `channel_conv`. It is concatenation `channel` and `conversion` (channelName_0 or chanelName_1)
    This metric is useful for `path_creator_udf` to build users paths(chains)
    */
    val data_union = data_preprocess_1.withColumn("channel_conv",concat($"channel",lit(GLUE_SYMBOL),$"conversion"))

    //`touch_data` it is data unit (Map) when ClientID got in touch with a certain channel
    val data_touch = data_union.withColumn("touch_data",map($"channel_conv",$"HitTimeStamp"))

    val data_group = data_touch.groupBy($"ClientID").agg(collect_list($"touch_data").as("touch_data_arr"))

    //For each ClientID exists `touch_data_arr` - the sequence of `touch_data` e.g User path , from first contact with channel till last
    val data_touchTube = data_group.select(
      $"ClientID",
      searchInception_udf($"touch_data_arr",lit(date_pure(0)),lit(NO_CONVERSION_SYMBOL)).as("touch_data_arr"))
      .cache()

    //---TIME---

    val data_bulk_HTS = data_touchTube.select(
      $"ClientID",
      channelTube_udf($"touch_data_arr").as("channel_seq"),
      htsTube_udf($"touch_data_arr").as("hts_seq"))


    val data_seqHTS = data_bulk_HTS.select(
      $"ClientID",
      path_creator_udf($"channel_seq",lit("success")).as("channel_paths_arr"),
      path_creator_udf($"hts_seq",lit("success")).as("hts_paths_arr")
    )

    val data_pathInfo = data_seqHTS.withColumn("path_zip_hts",explode(arrays_zip($"channel_paths_arr",$"hts_paths_arr"))).
      select(
        $"ClientID",
        $"path_zip_hts.channel_paths_arr".as("user_path"),
        $"path_zip_hts.hts_paths_arr".as("timeline")
      )

    val data_agg = data_pathInfo.
      groupBy("user_path").
      agg(count($"ClientID").as("count")).
      sort($"count".desc)

    data_pathInfo.coalesce(1).
        write.format("csv").
        option("header","true").
        mode("overwrite").
        save("/home/eva91/Documents/OMD/hts")

    data_agg.coalesce(1).
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(output_path)



    //---TIME---

    //---CHANNEL---

//    val data_bulk_CHNL_cache = data_touchTube.select(
//      $"ClientID",
//      channelTube_udf($"touch_data_arr").as("channel_seq")
//    )
//
//    //Create user successful paths(chains)
//    val data_path_success = data_bulk_CHNL_cache.select(
//      $"ClientID",
//      path_creator_udf($"channel_seq",lit("success")).as("paths")
//    ).withColumn("status",lit(true))
////
//    //Create user failed paths(chains)
//    val data_path_fail = data_bulk_CHNL_cache.select(
//      $"ClientID",
//      path_creator_udf($"channel_seq",lit("fail")).as("paths")
//    ).withColumn("status",lit(false))
////
//    //Union all successfull and failed paths
//    val data_path = data_path_success.union(data_path_fail)
////
//    //Cache DataFrame. Cause we use it DataFrame twice
//    val data_path_explode = data_path.select($"status",explode($"paths").as("paths")).cache()
////
//    val total_conversion = data_path_explode.filter($"status" === true).count() //ACTION
////
//    //`status` column has Boolean type and consists of true and false values. We did pivot method to get `paths`|`true`|`false` columns
//    val result = data_path_explode.
//      groupBy($"paths").
//      pivot($"status").
//      agg(count($"status"))
////
//    val result_sorted = try {
//      result.sort($"true".desc)
//    } catch {
//      case impossible_to_sort : UnsupportedOperationException => result.withColumn("true",lit(null))
//      case _                  : Throwable                     => result.withColumn("true",lit(null))
//    }
////
//    //Add `share` column . `share` column signs chain share(contribution) in  total conversions
//    val result_withShare = result_sorted.withColumn("share", $"true" / lit(total_conversion))
////
//    val output_data = result_withShare.select(
//      $"paths",
//      $"true",
//      $"false",
//      $"share"
//    )
//
//    output_data.coalesce(1).
//      write.format("csv").
//      option("header","true").
//      mode("overwrite").
//      save(output_path)

    //---CHANNEL---

  }
}