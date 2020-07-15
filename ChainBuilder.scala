import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.time._
import CONSTANTS._



object ChainBuilder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    import spark.implicits._
    //Safe class contains correct input argument types

    val optionsMap  = argsPars(args, usage) //Parse input arguments from command line

    val validMap = argsValid(optionsMap) // Validation input arguments types and building Map

    val arg_value = ArgValue(
      validMap("date_start").asInstanceOf[String],
      validMap("date_tHOLD").asInstanceOf[String],
      validMap("date_finish").asInstanceOf[String],
      validMap("product_name").asInstanceOf[String],
      validMap("projectID").asInstanceOf[Long],
      validMap("target_numbers").asInstanceOf[Array[Long]],
      validMap("source_platform").asInstanceOf[Array[String]],
      validMap("achieve_mode").asInstanceOf[String],
      validMap("flat_path").asInstanceOf[Array[String]],
      validMap("output_path").asInstanceOf[String],
      validMap("output_pathD").asInstanceOf[String]
    )

    val date_tHOLD:Long = localDateToUTC(arg_value.date_tHOLD)

    // Seq with date_start(Unix Hit) & date_finish(Unix)
    val date_range:Vector[Long] = Vector(
      arg_value.date_start,
      arg_value.date_finish).map(localDateToUTC(_))

    //Check `date_finish` is greater than `date_start`
    val date_pure = date_range match {
      case Vector(a,b) if a < b  => date_range
      case Vector(a,b) if a >= b => throw new Exception("`date_start` is greater than `date_finish`")
      case _                     => throw new Exception("`date_range` must have type Vector[Long]")
    }

    val bonds = DateBond(date_pure(0),date_pure(1))

    //Check correct chronology input dates
    val validDatesOrder = date_tHOLD match {
      case correct if date_tHOLD <= bonds.start => true
      case _                           => throw new Exception("`date_tHOLD` must be less or equal than `date_start`")
    }

    val target_numbersCSV:String = arg_value.target_numbers.map(_.toString).mkString(",") //`target_numbersCSV` for writing to CSV format ,cause CSV does not support bigInt

    //REGISTRATION
    val path_creator_udf    = spark.udf.register("path_creator",pathCreator(_:Seq[String],_:String):Array[String])
    val channel_creator_udf = spark.udf.register("channel_creator",channel_creator(_:String,_:String,_:String,_:String,_:String,_:String,_:String):String)
    val searchInception_udf = spark.udf.register("searchInception",searchInception(_:Seq[Map[String,Long]],_:Long,_:String):Seq[Map[String,Long]])
    val htsTube_udf         = spark.udf.register("htsTube",htsTube(_:Seq[Map[String,Long]]):Seq[String])
    val channelTube_udf     = spark.udf.register("channelTube",channelTube(_:Seq[Map[String,Long]]):Seq[String])
    //REGISTRATION

    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(arg_value.flat_path:_*)

    //Select significant metrics for further chain creation
    val data_work = data.select(
      $"ProjectID".cast(sql.types.LongType),
      $"ClientID".cast(sql.types.StringType),
      $"HitTimeStamp".cast(sql.types.LongType),
      $"ga_sourcemedium".cast(sql.types.StringType),
      $"utm_source".cast(sql.types.StringType),
      $"utm_medium".cast(sql.types.StringType),
      $"utm_campaign".cast(sql.types.StringType),
      $"adr_typenum".cast(sql.types.StringType),
      $"adr_profile".cast(sql.types.StringType),
      $"ga_location".cast(sql.types.StringType),
      $"goal".cast(sql.types.LongType),
      $"src".cast(sql.types.StringType)
    )

    //Customize data by client (`ProjectID`) and date range (`date_start` - `date_finish`)
    val data_custom_0 = data_work.
      filter($"HitTimeStamp" >= date_tHOLD && $"HitTimeStamp" < bonds.finish).
      filter($"ProjectID"    === arg_value.projectID)

    //Customize data by source (`source_platform`) and current product (`product_name`) if exists
    val data_custom_1 = arg_value.product_name match {
      case product if isEmpty(product) => data_custom_0.filter($"src".isin(arg_value.source_platform:_*))
      case product                     => data_custom_0.filter($"src".isin(arg_value.source_platform:_*)).
        filter($"ga_location" === arg_value.product_name)
    }

    //Create metric `channel` . It is indivisible unit for future  paths(chains)
    val data_preprocess_0 = data_custom_1.withColumn("channel", channel_creator_udf(
      $"src",
      $"ga_sourcemedium",
      $"utm_source",
      $"utm_medium",
      $"utm_campaign",
      $"adr_typenum",
      $"adr_profile")).select(
      $"ClientID",
      $"HitTimeStamp",
      $"goal",
      $"channel"
    )

    /*Retain only `target_numbers`, other goals convert to non-goals (`TRANSIT_ACTION`)
    Sort by `ClientID` and `HitTimeStamp`
    */
    val data_preprocess_1 = data_preprocess_0.
      withColumn("goal",when($"goal".isin(arg_value.target_numbers:_*),$"goal").otherwise(TRANSIT_ACTION)).
      sort($"ClientID", $"HitTimeStamp".asc).
      cache()

    //  Select `ClientID` which participated in conversions (`target_numbers`) in current period (`date_start` - `date_finish`)
    val actorsID = data_preprocess_1.
      filter($"HitTimeStamp" >= bonds.start && $"HitTimeStamp" < bonds.finish).
      filter($"goal".isin(arg_value.target_numbers:_*)).
      select($"ClientID").
      distinct()

    val data_bulk_0 = arg_value.achieve_mode match {

      case "true" => data_preprocess_1.as("df1").
        join(actorsID.as("df2"),($"df1.ClientID" === $"df2.ClientID"),"inner").
        select($"df1.*")

      case "false" => {val allID = data_preprocess_1.select($"ClientID").distinct()
        val notConvertedID = allID.except(actorsID)
        data_preprocess_1.as("df1").
          join(notConvertedID.as("df2"),($"df1.ClientID" === $"df2.ClientID"),"inner").
          select($"df1.*")
      }
    }

    //Create new metric `conversion`
    val data_bulk_1 = data_bulk_0.
      withColumn("conversion",when($"goal" =!= TRANSIT_ACTION,CONVERSION_SYMBOL).otherwise(NO_CONVERSION_SYMBOL)).
      select(
        $"ClientID",
        $"channel",
        $"conversion",
        $"HitTimeStamp"
      )

    //Create new metric `channel_conv`
    val data_union = data_bulk_1.withColumn("channel_conv",concat($"channel",lit(GLUE_SYMBOL),$"conversion"))

    //Create new metric `touch_data`. `touch_data` contains information about `ClientID` `HitTimeStamp` and the type of contact (`channel_conv`) with the channel
    val data_touch = data_union.withColumn("touch_data",map($"channel_conv",$"HitTimeStamp"))

    //Group `touch_data` in sequence by each `ClientID`
    val data_group = data_touch.groupBy($"ClientID").agg(collect_list($"touch_data").as("touch_data_arr"))

    val data_touchTube = data_group.select(
      $"ClientID",
      searchInception_udf($"touch_data_arr",lit(bonds.start),lit(NO_CONVERSION_SYMBOL)).as("touch_data_arr"))

    //Collect `channel` and `HitTimeStamp` sequences
    val data_seq = data_touchTube.select(
      $"ClientID",
      channelTube_udf($"touch_data_arr").as("channel_seq"),
      htsTube_udf($"touch_data_arr").as("hts_seq"))

    val data_chain = arg_value.achieve_mode match {
      case "true"  => data_seq.select(
        $"ClientID",
        path_creator_udf($"channel_seq",lit("success")).as("channel_paths_arr"),
        path_creator_udf($"hts_seq",lit("success")).as("hts_paths_arr")
      )

      case "false" => data_seq.select(
        $"ClientID",
        path_creator_udf($"channel_seq",lit("fail")).as("channel_paths_arr"),
        path_creator_udf($"hts_seq",lit("fail")).as("hts_paths_arr")
      )
    }

    //Detailed report about `ClientID` paths
    val data_pathInfo = data_chain.
      withColumn("path_zip_hts",explode(arrays_zip($"channel_paths_arr",$"hts_paths_arr"))).
      withColumn("target_numbers",lit(target_numbersCSV)).
      select(
        $"ClientID",
        $"path_zip_hts.channel_paths_arr".as("user_path"),
        $"path_zip_hts.hts_paths_arr".as("timeline"),
        $"target_numbers"
      ).cache()

    data_pathInfo.
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(arg_value.output_pathD)

    //Aggregated report about `ClientID` paths
    val data_agg = data_pathInfo.
      groupBy("user_path").
      agg(count($"ClientID").as("count")).
      sort($"count".desc)

    data_agg.coalesce(1).
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(arg_value.output_path)


  }
}
