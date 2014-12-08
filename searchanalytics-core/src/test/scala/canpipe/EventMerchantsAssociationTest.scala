package canpipe

import org.scalatest.{ BeforeAndAfter, FlatSpec }

class EventsMerchantsAssociationTest extends FlatSpec with BeforeAndAfter {

  before {

  }

  after {

  }

  "EventsMerchantsAssociation.fromString" should "return empty when no eventId" in {
    assert(
      EventsMerchantsAssociation.fromStrings(eventId = "",
        merchantIdsAsString = List.empty,
        merchantRanksAsString = List.empty,
        merchantDistancesAsString = List.empty,
        merchantLongitudesAsString = List.empty,
        merchantLatitudesAsString = List.empty,
        channels1 = List.empty,
        channels2 = List.empty,
        isNonAdRollupsAsString = List.empty,
        isRelevantHeadingsAsString = List.empty,
        isRelevantListingsAsString = List.empty,
        displayPositions = List.empty,
        zones = List.empty).isEmpty)
    assert(
      EventsMerchantsAssociation.fromStrings(eventId = "",
        merchantIdsAsString = List("12"),
        merchantRanksAsString = List.empty,
        merchantDistancesAsString = List.empty,
        merchantLongitudesAsString = List.empty,
        merchantLatitudesAsString = List.empty,
        channels1 = List.empty,
        channels2 = List.empty,
        isNonAdRollupsAsString = List.empty,
        isRelevantHeadingsAsString = List("true", "false"),
        isRelevantListingsAsString = List.empty,
        displayPositions = List.empty,
        zones = List.empty).isEmpty)
  }

  it should "return a structure the same size as the merchant-ids'" in {
    val merchantIds = List("34", "341p")
    assert(
      EventsMerchantsAssociation.fromStrings(eventId = "333",
        merchantIdsAsString = merchantIds,
        merchantRanksAsString = List.empty,
        merchantDistancesAsString = List.empty,
        merchantLongitudesAsString = List.empty,
        merchantLatitudesAsString = List.empty,
        channels1 = List.empty,
        channels2 = List.empty,
        isNonAdRollupsAsString = List.empty,
        isRelevantHeadingsAsString = List.empty,
        isRelevantListingsAsString = List.empty,
        displayPositions = List.empty,
        zones = List.empty).length == merchantIds.length)
  }

}
