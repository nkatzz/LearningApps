package ParseMaritime

import java.io.File

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.operation.distance.DistanceOp
import data._
import intervalTree.IntervalTree
import maritime.{HLEInterval, Havershine}
import oled.app.runutils.InputHandling.InputSource
import oled.app.runutils.RunningOptions

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

class ParseFileDataOptions(val HLE_Files_Dir: String, val LLE_File: String,
                      val allHLEs: List[String], val allLLEs: List[String], val runOpts: RunningOptions, val mode: String,
                      val addNoise: Boolean) extends InputSource

object ParseMaritime {
  // FOR NOW WILL NOT TAKE ARGUMENTS
  def writeDataToMongo(opts: ParseFileDataOptions): Unit = {
    // The path to the folder with RTEC results with HLE intervals. The generateIntervalTree
    // methods reads from those files (see the method).

    val coastFile = opts.runOpts.entryPath + "/Europe_Coastline_converted_WKT_Format.txt"

    val pathToCoastFile = coastFile

    val wkt_of_map = Source.fromFile(pathToCoastFile).getLines().next()

    val WKT_Reader: WKTReader = new WKTReader()
    val map_polygons: Geometry = WKT_Reader.read(wkt_of_map)

    val geom_factory: GeometryFactory = new GeometryFactory()

    // Right way to get distance
    val n_points = DistanceOp.nearestPoints(map_polygons, geom_factory.createPoint(new Coordinate(-4.3472633, 48.118046)))

    println(Havershine.haversine(n_points(0).y, n_points(0).x, n_points(1).y, n_points(1).x) * 1000) // in meters

    val pathToHLEIntervals = opts.HLE_Files_Dir

    // The path to the critical points (LLEs)
    val pathToLLEs = opts.LLE_File

    println("Generating intervals tree...")

    val intervalTree =
      generateIntervalTree(
        pathToHLEIntervals,
        opts.allLLEs ++ opts.allHLEs // Because there are some HLEs in allLLEs
      )

    // mode is either "asp" or "mln"
    /**
      * Parses the input data into logical syntax and generates data mini-batches for training.
      *
      * A data batch is a chunk of input data of given size. Size is measured by temporal duration,
      * so given batchSize = n, a mini-batch consists of input data with time stamps t to t+n.
      *
      */


    val data = Source.fromFile(pathToLLEs).getLines.filter(x =>
      opts.allLLEs.contains(x.split("\\|")(0))
      //!x.startsWith("coord") && !x.startsWith("velocity") && !x.startsWith("entersArea") && !x.startsWith("leavesArea")
    ).toIterator

    val coordData = Source.fromFile(pathToLLEs).getLines().filter(x => x.split("\\|")(0) == "coord").toIterator

    val mongoClient = MongoClient()
    mongoClient.dropDatabase("maritime")   // TO MAKE THE dbName BE GIVEN AS AN ARGUMENT LATER
    val collection = mongoClient("maritime")("examples")
    collection.createIndex(MongoDBObject("time" -> 1))

    var prev_batch_timestamp: Long = 0
    var batchCount = 0

    // They are used to help me to get what it should to next batch
    var nextBatchLLEsAccum = scala.collection.mutable.SortedSet[String]()
    var nextBatch = new ListBuffer[(Long,String)]     //keeps (time,atom)

    var nextCoordDistanceMap = new mutable.HashMap[(String, String), Double]()

    while(data.hasNext) {

      val distanceThreshold = 10000

      var currentBatch = nextBatch
      var vesselDistanceMap = nextCoordDistanceMap

      var timesAccum = scala.collection.mutable.SortedSet[Long]()
      var coordTimesAccum = scala.collection.mutable.SortedSet[Long]()
      var llesAccum = nextBatchLLEsAccum

      nextCoordDistanceMap = new mutable.HashMap[(String, String), Double]()
      nextBatch = new ListBuffer[(Long,String)]
      nextBatchLLEsAccum = scala.collection.mutable.SortedSet[String]()

      val INF_TS = 2000000000

      //var curr_id = 0
      timesAccum += prev_batch_timestamp

      while ((timesAccum.size <= opts.runOpts.chunkSize) && (data.hasNext)) {
        val newLine = data.next()
        //println(newLine)

        val split = newLine.split("\\|")

        //println(split.mkString(" "))

        val time = split(1)
        val lle = split(0)
        val mmsi = split(3)

        if (!timesAccum.contains(time.toLong)) {
          timesAccum += time.toLong
        }

        if (opts.addNoise && !coordTimesAccum.contains(time.toLong) && !Set("entersArea", "leavesArea").contains(lle) &&
          timesAccum.size <= opts.runOpts.chunkSize) {
          vesselDistanceMap = vesselDistanceMap ++ nextCoordDistanceMap
          nextCoordDistanceMap = new mutable.HashMap[(String, String), Double]()

          coordTimesAccum += time.toLong
          var currCoordTime = time

          while (coordData.hasNext && currCoordTime.toLong <= time.toLong) {
            val coordCurrLine = coordData.next().split("\\|")

            currCoordTime = coordCurrLine(1).toString
            val currCoordVessel = coordCurrLine(3).toString

            val currCoordLong = coordCurrLine(4).toDouble
            val currCoordLat = coordCurrLine(5).toDouble

            val nearestPoints = DistanceOp.nearestPoints(map_polygons, geom_factory.createPoint(new Coordinate(currCoordLong, currCoordLat)))

            val currDistance = Havershine.haversine(nearestPoints(0).y, nearestPoints(0).x, nearestPoints(1).y, nearestPoints(1).x) * 1000 // in meters

            /*
                if (currCoordTime.toLong <= time.toLong || (timesAccum.size < batchSize)) {
                  vesselDistanceMap += ((currCoordVessel, currCoordTime) -> currDistance)
                } else {
                  nextCoordDistanceMap += ((currCoordVessel, currCoordTime) -> currDistance)
                }
                 */

            if (currCoordTime.toLong <= time.toLong) {
              vesselDistanceMap += ((currCoordVessel, currCoordTime) -> currDistance)
            } else {
              nextCoordDistanceMap += ((currCoordVessel, currCoordTime) -> currDistance)
            }
          }
        }

        if (timesAccum.size > opts.runOpts.chunkSize) {
          nextBatchLLEsAccum += lle
          nextBatch += Tuple2(time.toLong, generateLLEInstances(newLine, opts.mode))
        } else {
          if (!llesAccum.contains(lle)) llesAccum += lle

          val currKey = (mmsi.toString, time.toString)

          val vesselDistanceMapKeys = vesselDistanceMap.keySet

          if (opts.addNoise && vesselDistanceMapKeys.contains(currKey)) {
            val currDistance = vesselDistanceMap(currKey) // in meters

            val deleteProb = 1.1 //1 - (distanceThreshold / currDistance)

            if (Random.nextDouble <= deleteProb) {
              currentBatch = currentBatch
            } else {
              currentBatch += Tuple2(time.toLong, generateLLEInstances(newLine, opts.mode))
            }
          } else {
            currentBatch += Tuple2(time.toLong, generateLLEInstances(newLine, opts.mode))
          }

        }
      }

      val json_time = prev_batch_timestamp

      //currentBatch += generateLLEInstances(newLine, mode)
      batchCount += 1
      println(batchCount)
      val intervals = if (data.hasNext) intervalTree.range(prev_batch_timestamp, timesAccum.last) else intervalTree.range(prev_batch_timestamp, INF_TS)

      if (!data.hasNext) timesAccum += INF_TS

      var extras: List[(Long,String)] = intervals.flatMap((interval) => {
        if (interval._3.stime >= timesAccum.head) {
          if (opts.allLLEs.contains(interval._3.hle)) {
            timesAccum += interval._3.stime
          }
          HLEIntervalToAtom(interval._3, interval._3.stime.toString, "None", opts.allHLEs)
        } else {
          List((1.toLong,"None"))
        }
      })

      extras = extras ++ intervals.flatMap((interval) => {
        if (interval._3.etime <= (timesAccum - timesAccum.last).last) {
          if (opts.allLLEs.contains(interval._3.hle)) {
            timesAccum += interval._3.stime
          }

          HLEIntervalToAtom(interval._3, interval._3.etime.toString, "None", opts.allHLEs)
        } else List((1.toLong,"None"))
      })

      //what is the use of this line?
      val nexts = timesAccum.sliding(2).map(x => if (opts.mode == "asp") (x.last.toLong, s"next(${x.last},${x.head})") else (x.last.toLong, s"next(${x.last},${x.head})"))

      /*
        var nextsHashMap = new mutable.HashMap[Long, Long]()
        var slideIterator = timesAccum.sliding(2)

        while (slideIterator.hasNext) {
          val currSortedSet = slideIterator.next()
          val currKey: Long = currSortedSet.head
          val currVal: Long = currSortedSet.last

          nextsHashMap += (currKey -> currVal)
        }
        */

      prev_batch_timestamp = timesAccum.last

      extras = extras ++ (timesAccum - timesAccum.last).flatMap { timeStamp =>
        val containedIn = intervals.filter(interval => (//(opts.allHLEs.contains(interval._3.hle) && interval._3.stime < nextsHashMap(timeStamp)
          //&& nextsHashMap(timeStamp) < interval._3.etime) ||
          (/*opts.allLLEs.contains(interval._3.hle) &&*/ interval._3.stime < timeStamp && timeStamp < interval._3.etime)))

        containedIn.flatMap(x => {
          if (opts.addNoise && opts.allLLEs.contains(x._3.hle)) {
            val currKey = (x._3.vessels(0), timeStamp.toString)

            val vesselDistanceMapKeys = vesselDistanceMap.keySet

            if (vesselDistanceMapKeys.contains(currKey)) {
              val currDistance = vesselDistanceMap(currKey) // in meters

              val deleteProb = 1.1 //1 - (distanceThreshold / currDistance)

              if (Random.nextDouble <= deleteProb) {
                List()
              } else {
                HLEIntervalToAtom(x._3, timeStamp.toString, "None" /*nextsHashMap(timeStamp).toString*/ , opts.allHLEs)
              }
            } else {
              HLEIntervalToAtom(x._3, timeStamp.toString, "None" /*nextsHashMap(timeStamp).toString*/ , opts.allHLEs)
            }
          } else {
            HLEIntervalToAtom(x._3, timeStamp.toString, "None" /*nextsHashMap(timeStamp).toString*/ , opts.allHLEs) // If I dont add noise I just need this line
          }
        })
      } toList

      // Why this line is used?
      if (extras.nonEmpty) {
        val stop = "stop"
      }

      for (x <- extras) currentBatch += x
      for (x <- nexts) currentBatch += x

      val all_events = currentBatch.filter(x => x != (1,"None"))

      var annotation = ListBuffer[(Long,String)]()
      var narrative = ListBuffer[(Long,String)]()

      val hleIter: Iterator[String] = opts.allHLEs.iterator
      val lleIter: Iterator[String] = opts.allLLEs.iterator

      while (hleIter.hasNext) {
        var currEvent = hleIter.next()
        annotation = annotation ++ all_events.filter(x => x._2.startsWith("holdsAt(" + currEvent + "("))
      }

      while (lleIter.hasNext) {
        var currEvent = lleIter.next()
        narrative = narrative ++ all_events.filter(x => x._2.startsWith("happensAt(" + currEvent + "("))
      }

      var timesEventsSortedMap = mutable.SortedMap[Long, (ListBuffer[String], ListBuffer[String])]() // It will be time->(annotation,narrative)

      val currAnnotationIterator = annotation.map(x => x._1).toSet.toIterator
      while(currAnnotationIterator.hasNext){
        val currTimestamp = currAnnotationIterator.next()

        if(!timesEventsSortedMap.keySet.contains(currTimestamp)){
          timesEventsSortedMap += (currTimestamp -> (annotation.filter(x => x._1 == currTimestamp).map(x => x._2), ListBuffer[String]()))
        }
        else{
          timesEventsSortedMap(currTimestamp)._1 ++= annotation.filter(x => x._1 == currTimestamp).map(x => x._2)
        }
      }

      val currNarrativeIterator = narrative.map(x => x._1).toSet.toIterator
      while(currNarrativeIterator.hasNext){
        val currTimestamp = currNarrativeIterator.next()

        if(!timesEventsSortedMap.keySet.contains(currTimestamp)){
          timesEventsSortedMap += (currTimestamp -> (ListBuffer[String](), narrative.filter(x => x._1 == currTimestamp).map(x => x._2)))
        }
        else{
          timesEventsSortedMap(currTimestamp)._2 ++= narrative.filter(x => x._1 == currTimestamp).map(x => x._2)
        }
      }

      for(currTimestamp: Long <- timesEventsSortedMap.keySet.toList) {
        val currAnnotation = timesEventsSortedMap(currTimestamp)._1
        val currNarrative  = timesEventsSortedMap(currTimestamp)._2

        val entry = MongoDBObject("time" -> currTimestamp.toInt, "annotation" -> currAnnotation, "narrative" -> currNarrative)
        collection.insert(entry)
      }
      //val curr_exmpl = Example(narrative.map(x => x._2).toList, annotation.map(x => x._2).toList, json_time.toString)
    }
  }

  // mode is either "asp" or "mln"
  def generateLLEInstances(line: String, mode: String) = {
    // These have a different schema
    // proximity is HLE
    val abnormalLLEs = Set[String]("coord", "entersArea", "leavesArea", "velocity")
    val split = line.split("\\|")
    if (!abnormalLLEs.contains(split(0))) {

      // These have a common simple schema:
      // change_in_heading, change_in_speed_start, change_in_speed_end,
      // gap_start, gap_end, slow_motion_start, slow_motion_end, stop_start, stop_end
      val lle = split(0)
      val time = split(1)
      val vessel = split(3)
      if (mode == "asp") s"happensAt($lle($vessel),$time)" else s"HappensAt(${lle.capitalize}_$vessel),$time)"
    } else {

      if (split(0) == "coord") {
        //coord|1443686670|1443686670|228041600|-4.47298500000000043286|48.38163999999999731472
        /*
        val lle = split(0)
        val time = split(1)
        val vessel = split(3)
        val lon = split(4)
        val lat = split(5)
        // Don't return nothing in the MLN case (can't handle the coords syntax)
        if ("mode" == "asp") s"happensAt($lle($vessel,$lon,$lat),$time)" else ""
        */
        // do nothing (we won't use coord).
        "None"
      } else if (split(0) == "entersArea" || split(0) == "leavesArea") {
        //entersArea|1443875806|1443875806|228017700|18515
        // For me enters and leaves are has the form
        //['Fluent','Timestamp','Area_ID','Vessel_ID']

        val lle = split(0)
        val time = split(1)
        val vessel = split(3)
        val area = split(2)
        if (mode == "asp") s"happensAt($lle($vessel,$area),$time)"
        else s"HappensAt(${lle.capitalize}_${vessel}_$area,$time)"

      } else if (split(0) == "velocity") {

        // do nothing (we won't use velocity)
        "None"

      } else {
        throw new RuntimeException(s"Unexpected event: $line")
      }
    }
  }

  /* Generate an HLE logical atom. The i var carries all the info, the t var is the particular
   * time point of the generated atom. "target" is the name of the target complex event. The
   * target event is turned into a holdsAt predicate, while all others are turned into happensAt predicates. */
  def HLEIntervalToAtom(i: HLEInterval, t: String, next_t: String, considered_HLEs: List[String] /*target: String*/ ): List[(Long,String)] = {

    val functor = if (considered_HLEs.contains(i.hle)) "holdsAt" else "happensAt"

    val fluentTerm =
      if (i.value != "true") s"${i.hle}(${(i.vessels :+ i.value).mkString(",")})"
      else s"${i.hle}(${i.vessels.mkString(",")})"

    val timestamp = {
      if (next_t == "None" || !considered_HLEs.contains(i.hle)) {
        t
      } else {
        next_t
      }
    }

    val fluentTerm2 = {
      if (i.hle == "rendezVous" //|| i.hle == "proximity"
        || i.hle == "pilotBoarding" || i.hle == "tugging") {

        if (i.value != "true") s"${i.hle}(${(i.vessels.reverse :+ i.value).mkString(",")})"
        else s"${i.hle}(${i.vessels.reverse.mkString(",")})"
      } else "None"
    }

    if (fluentTerm2 != "None") {
      List[(Long, String)]((timestamp.toLong, s"$functor($fluentTerm,$timestamp)"), (timestamp.toLong, s"$functor($fluentTerm2,$timestamp)"))
    } else {
      List[(Long, String)]((timestamp.toLong, s"$functor($fluentTerm,$timestamp)"))
    }
  }

  def generateIntervalTree(pathToHLEs: String, interestedIn: List[String]) = {

    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        println(d.listFiles.toList)
        d.listFiles.filter(f => f.isFile).toList
        // This line does not suits me
        // d.listFiles.filter(f => f.isFile && interestedIn.exists(eventName => f.getName.contains(eventName))).toList
      } else {
        List[File]()
      }
    }

    val files = getListOfFiles(pathToHLEs).map(_.getCanonicalPath)

    val intervalTree = new IntervalTree[HLEInterval]()

    var counter = 0
    files foreach { file =>
      println(s"Updating interval tree from $file")

      val data = Source.fromFile(file).getLines.filter(line => interestedIn.contains(line.split("\\|")(0)))

      while (data.hasNext) {
        val newLine = data.next()
        val i = HLEInterval(newLine)

        counter += 1

        //println(counter)
        //println(newLine)
        intervalTree.addInterval(i.stime, i.etime, i)
      }
    }

    intervalTree
  }

}
