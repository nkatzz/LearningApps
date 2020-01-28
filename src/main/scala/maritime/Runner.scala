package maritime

/**
 * Created by nkatz at 28/1/20
 */
object Runner extends App {

  val all_LLEs = List("gap_end", "coord", "velocity", "change_in_heading", "entersArea",
    "stop_start", "change_in_speed_start", "gap_start", "change_in_speed_end",
    "stop_end", "leavesArea", "slow_motion_end", "slow_motion_start")

  val all_HLEs = List("withinArea","tuggingSpeed","stopped","highSpeedNC","movingSpeed"
    ,"underWay","proximity","anchoredOrMoored","changingSpeed","gap"
    ,"lowSpeed","trawlingMovement","trawlSpeed","drifting","loitering"
    ,"sarMovement","sarSpeed","rendezVous","pilotBoarding","trawling","sar"
    ,"tugging")

}
