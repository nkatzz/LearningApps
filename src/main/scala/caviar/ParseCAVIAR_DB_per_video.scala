package caviar

/**
 * Created by nkatz at 30/1/20
 */

import java.io.File
import ParseCAVIAR._
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import scala.collection.immutable.SortedMap

object ParseCAVIAR_DB_per_video {

  def main(args: Array[String]) = {
    //val dataPath = "/home/nkatz/dev/CAVIAR-abrupt-original"
    val dataPath = args(0)
    //runOld(dataPath)
    runNew(dataPath)
  }


  /**
   * This is the new way to pair the data, where each example is the narrative
   * at some time point and the annotation at the next time point.
   * */
  def runNew(path: String) = {

    val mongoClient = MongoClient()

    val d = new File(path)
    val innerFolders = d.listFiles.sortBy(_.getName.split("-")(0).toInt)

    var videoCounter = 0

    for (f <- innerFolders) {
      videoCounter += 1
      println(s"Parsing video ${f.getCanonicalPath}")
      val files = f.listFiles.filter(x => dataFileNames.exists(p => x.getName.contains(p)))

      val contents =
        (for (f <- files)
          yield scala.io.Source.fromFile(f).getLines().filter(p => !p.startsWith("%"))).
          toList.flatten.mkString.replaceAll("\\s", "").split("\\."
        ).toList

      val parsed = contents.flatMap(x => parseAll(caviarParser(0), x).getOrElse(List(""))).filter(_ != "").asInstanceOf[List[Atom]]

      val atoms = SortedMap[Int, List[Atom]]() ++ parsed.groupBy(_.time.toInt)

      val (narrativeMap, annotationMap) = atoms.foldLeft(SortedMap[Int, List[Atom]](), SortedMap[Int, List[Atom]]()) { (maps, y) =>
        val (time, interpretation) = (y._1, y._2)
        val (narrativeAtoms, annotationAtoms) = interpretation.foldLeft(List[Atom](), List[Atom]()) { (lists, atom) =>
          atom match {
            case _: NarrativeAtom => (lists._1 :+ atom, lists._2)
            case _: AnnotationAtom => (lists._1, lists._2 :+ atom)
          }
        }
        (maps._1 + (time -> narrativeAtoms), maps._2 + (time -> annotationAtoms))
      }

      var hasMeeting = false
      var hasMoving = false

      val dbEntries = narrativeMap.foldLeft(Vector[DBObject]()) { (entries, narrativeMapRecord) =>

        val (narrativeTime, narrativeAtoms) = (narrativeMapRecord._1, narrativeMapRecord._2)
        val narrative = narrativeAtoms.flatMap(_.atoms)

        val key = narrativeTime + 40

        // The only case where the annotation map may not have the key is if narrativeTime
        // is the last time point in a video.
        val annotation = if (annotationMap.keySet.contains(key)) annotationMap(key).flatMap(_.atoms) else Nil

        if (annotation.exists(p => p.contains("meeting"))) hasMeeting = true
        if (annotation.exists(p => p.contains("moving"))) hasMoving = true

        val entry = MongoDBObject("time" -> narrativeTime) ++ ("annotation" -> annotation) ++ ("narrative" -> narrative)
        entries :+ entry
      }

      val dbName =
        if (hasMeeting && hasMoving) s"caviar-video-$videoCounter-meeting-moving"
        else if (hasMeeting) s"caviar-video-$videoCounter-meeting"
        else if (hasMoving) s"caviar-video-$videoCounter-moving"
        else s"caviar-video-$videoCounter"

      mongoClient.dropDatabase(dbName)
      val collection = mongoClient(dbName)("examples")

      println(s"Inserting data in $dbName")

      dbEntries foreach (entry => collection.insert(entry))

    }
  }



  /**
   * This is the old way to pair the data, where each example is the narrative
   * and the annotation at one particular time point.
   * */
  def runOld(path: String) = {

    val mongoClient = MongoClient()

    val d = new File(path)
    val innerFolders = d.listFiles.sortBy(_.getName.split("-")(0).toInt)

    //var lastTime = 0

    var videoCounter = 0

    for (f <- innerFolders) {
      videoCounter += 1
      println(s"Parsing video ${f.getCanonicalPath}")
      val files = f.listFiles.filter(x => dataFileNames.exists(p => x.getName.contains(p)))
      val contents =
        (for (f <- files)
          yield scala.io.Source.fromFile(f).getLines().filter(p => !p.startsWith("%"))).
          toList.flatten.mkString.replaceAll("\\s", "").split("\\."
        ).toList

      val parsed = contents.flatMap(x => parseAll(caviarParser(0), x).getOrElse(List(""))).filter(_ != "").asInstanceOf[List[Atom]]

      val atoms = SortedMap[Int, List[Atom]]() ++ parsed.groupBy(_.time.toInt)

      var hasMeeting = false
      var hasMoving = false

      val dbEntries = atoms.foldLeft(Vector[DBObject]()) { (entries, mapRecord) =>
        val (time, atoms) = (mapRecord._1, mapRecord._2)
        val narrative = atoms.filter(x => !x.annotationAtom).flatMap(z => z.atoms)
        val annotation = atoms.filter(x => x.annotationAtom).flatMap(z => z.atoms)

        if (annotation.exists(p => p.contains("meeting"))) hasMeeting = true
        if (annotation.exists(p => p.contains("moving"))) hasMoving = true

        val entry = MongoDBObject("time" -> time) ++ ("annotation" -> annotation) ++ ("narrative" -> narrative)
        entries :+ entry
      }

      val dbName =
        if (hasMeeting && hasMoving) s"caviar-video-$videoCounter-meeting-moving"
        else if (hasMeeting) s"caviar-video-$videoCounter-meeting"
        else if (hasMoving) s"caviar-video-$videoCounter-moving"
        else s"caviar-video-$videoCounter"

      mongoClient.dropDatabase(dbName)
      val collection = mongoClient(dbName)("examples")

      println(s"Inserting data in $dbName")

      dbEntries foreach (entry => collection.insert(entry))

    }
  }

}
