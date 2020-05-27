object CONSTANTS {
  //CONSTANT
  val DATE_PATTERN = "[12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])".r // Pattern for RegEx . Check correct string `Date` or not
  val DATE_UNIX_TIME_STAMP = new java.text.SimpleDateFormat("yyyy-MM-dd") // Pattern to convert date(String) into Unix Time Stamp
  val TRANSIT_ACTION:Long = -1 //Indicate the action was not a conversion
  //CONSTANT

  //UDF
  def pathCreator(arr:Seq[String],
                  contact_pos : String = "_1",
                  concat_neg :  String = "_0",
                  transit : String     = "=>"
                 ) : Array[String] = {
    //Create user paths

    val touchpoints = arr.mkString(transit)
    val paths = touchpoints.split(contact_pos).map(_.trim)
    val success_paths = paths.filterNot(_.endsWith(concat_neg)).map(_.stripPrefix(transit))
    val chains = success_paths.map(_.replace(concat_neg,""))
    chains
  }

  def channel_creator(
                       src:String,
                       ga_sourcemedium:String,
                       utm_source:String,
                       utm_medium:String,
                       adr_typenum:String,
                       adr_profile:String) = {

    val channel = src match {
      case "adriver" if adr_typenum == "0" => "view" + ":" + utm_source + " / " + utm_medium + ":" + adr_profile.toString
      case "adriver" if adr_typenum != "0" => "click" + ":" + utm_source + " / " + utm_medium + ":" + adr_profile.toString
      case "seizmik"                     => "seizmik_channel" //ALLERT NEED TO EDIT IN FUTURE!!!
      case "ga" | "bq"                  => ga_sourcemedium
      case _                             => throw new Exception("Unknown data source")
    }
    channel

  }


  def DateStrToUnix(date:String):Long = {
    val date_correct = DATE_PATTERN.findFirstIn(date) match {
      case Some(s) => DATE_UNIX_TIME_STAMP.parse(s).getTime()
      case _ => throw new Exception("Incorrect Date Format.Use YYYY-MM_dd format")}
    date_correct
    }

  def isEmpty(x:String) = x == null || x.isEmpty

  }


case class Jvalue(date_start:String,
                  date_finish:String,
                  product_name:String,
                  projectID:Long,
                  target_numbers:Array[Long],
                  source_platform:Array[String],
                  flat_path:String
                 )
