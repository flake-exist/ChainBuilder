import org.apache.spark._
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext._
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession

import CONSTANTS._

object ChainBuilder_v2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._

    val optionsMap  = argsPars(args, usage) //Parse input arguments from command line

    val arg_keys = optionsMap.keys.toList //Extract keys (--option)

    //check if each argument is in necessary argument list
    val args_status = necessary_args.map(arg_keys.contains(_)).forall(_ == true) match {
      case true => "correct"
      case _    => throw new Exception(usage)
    }
    //----------CHECK ARGS TYPES----------
    val target_numbers   = try {
      optionsMap("target_numbers").stripPrefix("[").stripSuffix("]").split(",").map(_.toLong)
    } catch {
      case _: Throwable => throw new Exception("target_numbers : Convert error")
    }

    val source_platform  = try {
      optionsMap("source_platform").stripPrefix("[").stripSuffix("]").split(",").map(_.toString)
    } catch {
      case _ : Throwable => throw new Exception("source_platform : Convert error")
    }

    val projectID        = try {optionsMap("projectID").toLong
    } catch {
      case _ : Throwable => throw new Exception("projectID : Convert error")
    }

    val flat_path        = try {optionsMap("flat_path").stripPrefix("[").stripSuffix("]").split(",").map(_.toString)
    } catch {
      case _ : Throwable => throw new Exception("flat_path : Convert error")
    }
    //----------CHECK ARGS TYPES----------

    val arg_value = ArgValue(
      optionsMap("date_start"),
      optionsMap("date_finish"),
      optionsMap("product_name"),
      projectID,
      target_numbers,
      source_platform,
      flat_path,
      optionsMap("output_path")
    )

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
    val target_goal = arg_value.target_numbers :+ TRANSIT_ACTION //`TRANSIT_ACTION ` defined in `CONSTANTS` module

    //REGISTRATION
    val path_creator_udf = spark.udf.register("path_creator",pathCreator(_:Seq[String],_:String):Array[String])
    val channel_creator_udf = spark.udf.register("channel_creator",channel_creator(_:String,_:String,_:String,_:String,_:String,_:String):String)
    //REGISTRATION

    //Connect to data
    //    val data = spark.read.
    //      format("csv").
    //      option("header","true").
    //      option("delimiter",";").
    //      load(json_config.flat_path)

    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      load(flat_path:_*)

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

    data_work.printSchema()

    //Customize data by client (`ProjectID`) and date range (`date_start` - `date_finish`)
    val data_custom_0 = data_work.
      filter($"HitTimeStamp" >= date_pure(0) && $"HitTimeStamp" < date_pure(1)).
      filter($"ProjectID"    === arg_value.projectID)

    //Customize data by source (`source_platform`) and current product (`product_name`) if exists
    val data_custom_1 = arg_value.product_name match {
      case product if isEmpty(product) => data_custom_0.filter($"src".isin(arg_value.source_platform:_*))
      case product                     => data_custom_0.filter($"src".
        isin(arg_value.source_platform:_*)).
        filter($"ga_location" === arg_value.product_name)
    }
    //Create metric `channel` . It is indivisible unit for future  paths(chains)
    val data_custom_2 = data_custom_1.withColumn("channel", channel_creator_udf(
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

    /*
    Filter by `target_numbers`
    Sort by `ClientID` and `HitTimeStamp` (ascending) to build in future user(`ClientID`) path (chronological sequence of touchpoints(channels)
    */
    val data_preprocess_0 = data_custom_2.
      withColumn("goal",when($"goal".isNull,TRANSIT_ACTION).otherwise($"goal")).
      filter($"goal".isin(target_goal:_*)).
      sort($"ClientID", $"HitTimeStamp".asc)

    /*Firstly create new metric `conversion`. Was conversion or not
    Secondly, check if `channel` contains null values
    */
    val data_preprocess_1 = data_preprocess_0.
      withColumn("conversion",when($"goal" === TRANSIT_ACTION,NO_CONVERSION_SYMBOL).otherwise(CONVERSION_SYMBOL)).select(
      $"ClientID",
      $"channel",
      $"conversion",
      $"HitTimeStamp"
    )

    /* Create metric `clutch`. It is concatenation `channel` and `conversion` (channelName_0 or chanelName_1)
    This metric is usefull for `path_creator_udf` to build users paths(chains)
    */
    val data_union = data_preprocess_1.withColumn("clutch",concat($"channel",lit(GLUE_SYMBOL),$"conversion"))

    //Collect user chronological sequence of `clutch` e.g. channels with knowledge got this channel conversion or not
    val data_assembly = data_union.select(
      $"ClientID",
      $"clutch").
      groupBy($"ClientID").
      agg(collect_list($"clutch").as("clutch_arr"))

    val data_assembly_cache = data_assembly.cache()

    //Create user successful paths(chains)
    val data_path_success = data_assembly_cache.select(
      $"ClientID",
      path_creator_udf($"clutch_arr",lit("success")).as("paths")
    ).withColumn("status",lit(true))

    //Create user failed paths(chains)
    val data_path_fail = data_assembly_cache.select(
      $"ClientID",
      path_creator_udf($"clutch_arr",lit("fail")).as("paths")
    ).withColumn("status",lit(false))

    //Union all successfull and failed paths
    val data_path = data_path_success.union(data_path_fail)

    //Cache DataFrame. Cause we use it DataFrame twice
    val data_path_explode = data_path.select($"status",explode($"paths").as("paths")).cache()

    val total_conversion = data_path_explode.filter($"status" === true).count()

    //`status` column has Boolean type and consists of true and false values. We did pivot method to get `paths`|`true`|`false` columns
    val result = data_path_explode.
      groupBy($"paths").
      pivot($"status").
      agg(count($"status"))

    val result_sorted = try {
      result.sort($"true".desc)
    } catch {
      case impossible_to_sort : UnsupportedOperationException => result.withColumn("true",lit(null))
      case _                  : Throwable                     => result.withColumn("true",lit(null))
    }

    //Add `share` column . `share` column signs chain share(contribution) in  total conversions
    val result_withShare = result_sorted.withColumn("share", $"true" / lit(total_conversion))

//    result_withShare.show(10)

    val output_data = result_withShare.select(
      $"paths",
      $"true",
      $"false",
      $"share"
    )

    output_data.coalesce(1).
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(output_path)

  }
}