package canpipe

import scala.util.control.Exception._
import util.{ Base => Util }
import util.TwoTableAssociation

object EventsMerchantsAssociation {
  def fromStrings(eventId: String,
                  merchantIdsAsString: List[String],
                  merchantRanksAsString: List[String],
                  merchantDistancesAsString: List[String],
                  merchantLongitudesAsString: List[String],
                  merchantLatitudesAsString: List[String],
                  channels1: List[String],
                  channels2: List[String],
                  isNonAdRollupsAsString: List[String],
                  isRelevantHeadingsAsString: List[String],
                  isRelevantListingsAsString: List[String],
                  displayPositions: List[String],
                  zones: List[String]): List[EventsMerchantsAssociation] = {
    def filterBy[T](mIdsOpt: List[Option[Long]], stringsToFilter: List[String], f: String => Option[T], default: T): List[T] = {
      val TOpts = Util.fillToMinimumSize(merchantRanksAsString.map(s => f(s)), mIdsOpt.length, None)
      (mIdsOpt zip TOpts).filter { case (mIdOpt, _) => mIdOpt.isDefined }.map { case (_, mRankOpt) => mRankOpt.getOrElse(default) }
    }
    val mIdsOpt = merchantIdsAsString.map { s =>
      catching(classOf[NumberFormatException]).opt { Some(s.toLong) }.getOrElse({
        catching(classOf[NumberFormatException]).opt { (s.substring(0, s.length - 1)).toLong }
      })
    }
    // merchant ids: only keep the ones that are OK
    val mIds = mIdsOpt.filter(_.isDefined).map(_.get)
    if (mIds.length != mIdsOpt.length) {
      println(s"Some merchant ids are not OK: ${merchantIdsAsString.mkString("{", ",", "}")}")
    }
    // NB: for everyone: discard the ones for which mId is invalid. Return a default if invalid
    val mRanks = filterBy(mIdsOpt, merchantRanksAsString, s => catching(classOf[NumberFormatException]).opt { s.toInt }, 0)
    val mDists = filterBy(mIdsOpt, merchantDistancesAsString, s => catching(classOf[NumberFormatException]).opt { s.toDouble }, 0.0)
    val mLongs = filterBy(mIdsOpt, merchantLongitudesAsString, s => catching(classOf[NumberFormatException]).opt { s.toDouble }, 0.0)
    val mLats = filterBy(mIdsOpt, merchantLatitudesAsString, s => catching(classOf[NumberFormatException]).opt { s.toDouble }, 0.0)
    val mNonAdRollups = filterBy(mIdsOpt, isNonAdRollupsAsString, s => Util.String.toBooleanOpt(s), false)
    val mRelevantHeadings = filterBy(mIdsOpt, isRelevantHeadingsAsString, s => Util.String.toBooleanOpt(s), false)
    val mRelevantListings = filterBy(mIdsOpt, isRelevantListingsAsString, s => Util.String.toBooleanOpt(s), false)
    apply(eventId,
      merchantIds = mIds,
      merchantRanks = mRanks,
      merchantDistances = mDists,
      merchantLongitudes = mLongs,
      merchantLatitudes = mLats,
      channels1,
      channels2,
      isNonAdRollups = mNonAdRollups,
      isRelevantHeadings = mRelevantHeadings,
      isRelevantListings = mRelevantListings,
      displayPositions,
      zones)

  }

  def apply(eventId: String,
            merchantIds: List[Long],
            merchantRanks: List[Int],
            merchantDistances: List[Double],
            merchantLongitudes: List[Double],
            merchantLatitudes: List[Double],
            channels1: List[String],
            channels2: List[String],
            isNonAdRollups: List[Boolean],
            isRelevantHeadings: List[Boolean],
            isRelevantListings: List[Boolean],
            displayPositions: List[String],
            zones: List[String]): List[EventsMerchantsAssociation] = {
    def loop(merchantIds: List[Long],
             merchantRanks: List[Int],
             merchantDistances: List[Double],
             merchantLongitudes: List[Double],
             merchantLatitudes: List[Double],
             channels1: List[String],
             channels2: List[String],
             isNonAdRollups: List[Boolean],
             isRelevantHeadings: List[Boolean],
             isRelevantListings: List[Boolean],
             displayPositions: List[String],
             zones: List[String],
             result: List[EventsMerchantsAssociation]): List[EventsMerchantsAssociation] = {
      merchantIds match {
        case eIds if eIds.isEmpty => result
        case _ =>
          loop(merchantIds.tail,
            merchantRanks.tail,
            merchantDistances.tail,
            merchantLongitudes.tail,
            merchantLatitudes.tail,
            channels1.tail,
            channels2.tail,
            isNonAdRollups.tail,
            isRelevantHeadings.tail,
            isRelevantListings.tail,
            displayPositions.tail,
            zones.tail,
            EventsMerchantsAssociation(
              eventId = eventId,
              merchantId = merchantIds.head,
              merchantRank = merchantRanks.head,
              merchantDistance = merchantDistances.head,
              merchantLongitude = merchantLongitudes.head,
              merchantLatitude = merchantLatitudes.head,
              channel1 = channels1.head,
              channel2 = channels2.head,
              isNonAdRollup = isNonAdRollups.head,
              isRelevantHeading = isRelevantHeadings.head,
              isRelevantListing = isRelevantListings.head,
              displayPosition = displayPositions.head,
              zone = zones.head) :: result)
      }

    }
    if (eventId.trim.isEmpty || merchantIds.isEmpty) List.empty
    else {
      // TODO: review value of defaults in following:
      val size = merchantIds.length
      loop(merchantIds,
        merchantRanks = Util.fillToMinimumSize(merchantRanks, size, -1),
        merchantDistances = Util.fillToMinimumSize(merchantDistances, size, 0L),
        merchantLongitudes = Util.fillToMinimumSize(merchantLongitudes, size, -1),
        merchantLatitudes = Util.fillToMinimumSize(merchantLatitudes, size, -1),
        channels1 = Util.fillToMinimumSize(channels1, size, ""),
        channels2 = Util.fillToMinimumSize(channels2, size, ""),
        isNonAdRollups = Util.fillToMinimumSize(isNonAdRollups, size, false),
        isRelevantHeadings = Util.fillToMinimumSize(isRelevantHeadings, size, false),
        isRelevantListings = Util.fillToMinimumSize(isRelevantListings, size, false),
        displayPositions = Util.fillToMinimumSize(displayPositions, size, ""),
        zones = Util.fillToMinimumSize(zones, size, ""),
        result = List.empty)
    }
  }

}

case class EventsMerchantsAssociation(
  eventId: String,
  merchantId: Long,
  merchantRank: Int,
  merchantDistance: Double,
  merchantLongitude: Double,
  merchantLatitude: Double,
  channel1: String,
  channel2: String,
  isNonAdRollup: Boolean,
  isRelevantHeading: Boolean,
  isRelevantListing: Boolean,
  displayPosition: String,
  zone: String) extends TwoTableAssociation[String, Long] {
  val fk1 = eventId
  val fk2 = merchantId
}

