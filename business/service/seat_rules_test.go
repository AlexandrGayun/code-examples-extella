package service

import (
	"fmt"
	"github.com/proj/business/domain"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type SeatRulesTestSuite struct {
	CommonSuite
	requestedSeatsGroupedByRowsGroupedBySpl           map[string]map[string][]domain.Seat
	requestedFragmentedSeatsGroupedByRowsGroupedBySpl map[string]map[string][]domain.Seat
}

func (suite *SeatRulesTestSuite) SetupSuite() {
	suite.InitCommon()
	newUserInput := domain.NewUser{
		Name:    suite.faker.Person().Name(),
		OrgName: fmt.Sprintf("test-org-%d", time.Now().Unix()),
		Email:   fmt.Sprintf("test-%d@entrello.io", time.Now().Unix()),
	}
	user, err := suite.service.CreateUserAndOrg(suite.ctx, newUserInput)
	suite.NoError(err)
	suite.user = user
	tg, err := suite.service.CreateTaxGroup(suite.ctx, &domain.IDs{OrgID: *user.OrgID, UserID: user.ID}, &domain.NewTaxGroup{Name: "tg1", TaxRate: float64(13)})
	suite.NoError(err)
	suite.taxGroup = tg
	pn, err := suite.service.CreatePriceName(suite.ctx, &domain.IDs{OrgID: *user.OrgID, UserID: user.ID}, &domain.NewPriceName{Name: "pn1", TaxGroupID: tg.ID})
	suite.NoError(err)
	suite.priceName = pn
	loc, err := suite.service.CreateLocation(suite.ctx, &domain.IDs{OrgID: *user.OrgID, UserID: user.ID}, &domain.NewLocation{Name: "loc1"})
	suite.NoError(err)
	suite.location = loc
	org, err := suite.service.GetOrgByID(suite.ctx, *user.OrgID)
	suite.NoError(err)
	suite.org = org
	_, _, seats := suite.CreateSeatingPlanEventWithSeatGroups(12, 6)
	//
	preparedSeats := make([]domain.Seat, 6)
	for i := 0; i < 6; i++ {
		preparedSeats[i] = seats[i]
	}
	//
	preparedFragmentedSeats := make([]domain.Seat, 2)
	for i := 0; i < 2; i++ {
		preparedFragmentedSeats[i] = seats[i+1]
	}
	//
	sgs, err := suite.service.GetSeatGroups(suite.ctx, &domain.SeatGroupsFilter{OrgID: &suite.org.ID,
		SeatingPlanID: &suite.spl.ID,
	})
	suite.NoError(err)
	preparedSeatsPerSplPerRow := map[string]map[string][]domain.Seat{
		suite.spl.ID: {
			sgs[0].ID: preparedSeats,
		},
	}
	suite.requestedSeatsGroupedByRowsGroupedBySpl = preparedSeatsPerSplPerRow
	preparedFragmentedSeatsPerSplPerRow := map[string]map[string][]domain.Seat{
		suite.spl.ID: {
			sgs[0].ID: preparedFragmentedSeats,
		},
	}
	suite.requestedFragmentedSeatsGroupedByRowsGroupedBySpl = preparedFragmentedSeatsPerSplPerRow
}

func (suite *SeatRulesTestSuite) TearDownSuite() {
	defer suite.testUtil.Teardown()
}

func (suite *SeatRulesTestSuite) TestGetRowSeatsForSeatingPlanAndRequestedSeats() {
	availableRowSeats, requestedRowsSeats, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	for _, rowSeats := range availableRowSeats {
		suite.Equal(6, len(rowSeats))
	}
	for _, rowIDSeatsMap := range suite.requestedSeatsGroupedByRowsGroupedBySpl {
		for rowID, seatsNums := range rowIDSeatsMap {
			// requestedRowsSeats is the same as requestedSeatsWithRowsGroupedBySpl but with discarded spl IDs
			suite.ElementsMatch(seatsNums, requestedRowsSeats[rowID], "should contain the same elements")
		}
	}
}

func (suite *SeatRulesTestSuite) TestGetSeatsWithPriceCategoriesForSpl() {
	seatsByPriceCategoriesPerSpl, err := getSeatsWithPriceCategoriesForSpl(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedSeatsGroupedByRowsGroupedBySpl, true)
	suite.NoError(err)
	for _, priceCategoriesSeatsCount := range seatsByPriceCategoriesPerSpl {
		suite.Equal(1, len(priceCategoriesSeatsCount))
		suite.Equal(int64(12), priceCategoriesSeatsCount[0].Count)
	}
}

func (suite *SeatRulesTestSuite) TestCheckEventFullGroupOrderingRestrictionFailed() {
	availableRowSeats, _, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	// take non full group seats of 2 with random row
	var anyRowID string
	nonfullGroupSeats := make([]domain.Seat, 2)
	for rowID, seats := range suite.requestedSeatsGroupedByRowsGroupedBySpl[suite.spl.ID] {
		nonfullGroupSeats[0] = seats[0]
		nonfullGroupSeats[1] = seats[1]
		anyRowID = rowID
		break
	}
	splIDBookAllSeatsInGroup := map[string]string{suite.spl.ID: "random event title"}
	err = checkEventFullGroupOrderingRestriction(anyRowID, availableRowSeats[anyRowID], nonfullGroupSeats, splIDBookAllSeatsInGroup, suite.requestedSeatsGroupedByRowsGroupedBySpl)
	suite.ErrorContains(err, "violate event restriction for event random event title, only full group seats ordering allowed")
}

func (suite *SeatRulesTestSuite) TestCheckEventFullGroupOrderingRestrictionSuccessful() {
	availableRowSeats, _, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	// take full group seats of 6 with random row
	var anyRowID string
	fullGroupSeats := make([]domain.Seat, 6)
	for rowID, seats := range suite.requestedSeatsGroupedByRowsGroupedBySpl[suite.spl.ID] {
		for i := 0; i < 6; i++ {
			fullGroupSeats[i] = seats[i]
		}
		anyRowID = rowID
		break
	}
	splIDBookAllSeatsInGroup := map[string]string{suite.spl.ID: "random event title"}
	err = checkEventFullGroupOrderingRestriction(anyRowID, availableRowSeats[anyRowID], fullGroupSeats, splIDBookAllSeatsInGroup, suite.requestedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
}

func (suite *SeatRulesTestSuite) TestCheckSeatsCountIsLowerThan10PercentsOfSeatingPlanSeatsFailed() {
	// seats are fragmented, but they count are greater than 10% of seating plan price category seats
	availableRowSeats, requestedRowsSeats, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedFragmentedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	var pcID string
	for _, seats := range requestedRowsSeats {
		if seats[0].PriceCategoryID != nil {
			pcID = *seats[0].PriceCategoryID
			break
		}
	}
	seatsByPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 4, PriceCategoryID: &pcID}, // make it more than number of fragmented requested seats(2) to not trigger last seats in seat group check
		}, // but greater than 3 to not trigger last seats available per price category + 1
	}
	// tamper allSeatsByPriceCategoriesPerSpl struct to imitate the 10% of price category presence
	allSeatsPerPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 19, PriceCategoryID: &pcID}, // any number lower than 40(available count * 10)
		},
	}
	for rowID, requestedRowSeatsNums := range requestedRowsSeats {
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeatsNums)
		suite.NoError(err)
		err = checkMappedRowForFragmentation(mappedRowSeats)
		suite.ErrorContains(err, "seating plan fragmentation detected")
		checkRes := skipFragmentationCheck(requestedRowSeatsNums, seatsByPriceCategoriesPerSpl, allSeatsPerPriceCategoriesPerSpl)
		suite.False(checkRes)
	}
}

func (suite *SeatRulesTestSuite) TestCheckSeatsCountIsLowerThan10PercentsOfSeatingPlanSeatsSuccessful() {
	// seats are fragmented, but they count are lower than 10% of seating plan price category seats
	availableRowSeats, requestedRowsSeats, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedFragmentedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	var pcID string
	for _, seats := range requestedRowsSeats {
		if seats[0].PriceCategoryID != nil {
			pcID = *seats[0].PriceCategoryID
			break
		}
	}
	seatsByPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 4, PriceCategoryID: &pcID}, // make it more than number of fragmented requested seats(2) to not trigger last seats in seat group check
		}, // but greater than 3 to not trigger last seats available per price category + 1
	}
	// tamper allSeatsByPriceCategoriesPerSpl struct to imitate the 10% of price category presence
	allSeatsPerPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 45, PriceCategoryID: &pcID}, // any number greater than 40(available count * 10)
		},
	}
	for rowID, requestedRowSeats := range requestedRowsSeats {
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeats)
		suite.NoError(err)
		err = checkMappedRowForFragmentation(mappedRowSeats)
		suite.ErrorContains(err, "seating plan fragmentation detected")
		checkRes := skipFragmentationCheck(requestedRowSeats, seatsByPriceCategoriesPerSpl, allSeatsPerPriceCategoriesPerSpl)
		suite.True(checkRes)
	}
}

func (suite *SeatRulesTestSuite) TestCheckSeatsCountIsEqualToAvailableMinusOneFailed() {
	// seats are fragmented, but they count are equal to remaining available seating plan price category seats minus one
	availableRowSeats, requestedRowsSeats, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedFragmentedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	var pcID string
	for _, seats := range requestedRowsSeats {
		if seats[0].PriceCategoryID != nil {
			pcID = *seats[0].PriceCategoryID
			break
		}
	}
	seatsByPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 4, PriceCategoryID: &pcID}, // make it more than number of fragmented requested seats(2) to not trigger last seats in seat group check
		}, // but greater than 3 to not trigger last seats available per price category + 1
	}
	allSeatsByPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 5, PriceCategoryID: &pcID}, // any number lower than 20(as we request 2 seats, 2*10(%) = 20
		},
	}
	for rowID, requestedRowSeats := range requestedRowsSeats {
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeats)
		suite.NoError(err)
		err = checkMappedRowForFragmentation(mappedRowSeats)
		suite.ErrorContains(err, "seating plan fragmentation detected")
		checkRes := skipFragmentationCheck(requestedRowSeats, seatsByPriceCategoriesPerSpl, allSeatsByPriceCategoriesPerSpl)
		suite.False(checkRes)
	}
}

func (suite *SeatRulesTestSuite) TestCheckSeatsCountIsEqualToAvailableMinusOneSuccessful() {
	// seats are fragmented, but they count are equal to remaining available seating plan price category seats minus one
	availableRowSeats, requestedRowsSeats, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedFragmentedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	var pcID string
	for _, seats := range requestedRowsSeats {
		if seats[0].PriceCategoryID != nil {
			pcID = *seats[0].PriceCategoryID
			break
		}
	}
	// tamper seatsByPriceCategoriesPerSpl struct to imitate these seats count are requested seats + 1
	seatsByPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 3, PriceCategoryID: &pcID}, // make it more than number of fragmented requested seats(2) to not trigger last seats in seat group check
		},
	}
	allSeatsByPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 5, PriceCategoryID: &pcID}, // any number greater than 30(available count * 10)
		},
	}
	for rowID, requestedRowSeats := range requestedRowsSeats {
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeats)
		suite.NoError(err)
		err = checkMappedRowForFragmentation(mappedRowSeats)
		suite.ErrorContains(err, "seating plan fragmentation detected")
		checkRes := skipFragmentationCheck(requestedRowSeats, seatsByPriceCategoriesPerSpl, allSeatsByPriceCategoriesPerSpl)
		suite.True(checkRes)
	}
}

func (suite *SeatRulesTestSuite) TestCheckSeatsAreTheLastInThePriceCategory() {
	// seats are fragmented, but they are last in the price category
	availableRowSeats, requestedRowsSeats, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedFragmentedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	var pcID string
	for _, seats := range requestedRowsSeats {
		if seats[0].PriceCategoryID != nil {
			pcID = *seats[0].PriceCategoryID
			break
		}
	}
	// tamper seatsByPriceCategoriesPerSpl struct to imitate these seats are the last seats in the price category
	seatsByPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 2, PriceCategoryID: &pcID},
		},
	}
	allSeatsByPriceCategoriesPerSpl := map[string][]domain.SeatsPerPriceCategories{
		suite.spl.ID: {
			{Count: 2, PriceCategoryID: &pcID},
		},
	}
	for rowID, requestedRowSeats := range requestedRowsSeats {
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeats)
		suite.NoError(err)
		err = checkMappedRowForFragmentation(mappedRowSeats)
		suite.ErrorContains(err, "seating plan fragmentation detected")
		checkRes := skipFragmentationCheck(requestedRowSeats, seatsByPriceCategoriesPerSpl, allSeatsByPriceCategoriesPerSpl)
		suite.True(checkRes)
	}
}

func (suite *SeatRulesTestSuite) TestMapAllRowSeatsForAvailability() {
	availableRowSeats, requestedRowsSeatsNums, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	for rowID, requestedRowSeatsNums := range requestedRowsSeatsNums {
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeatsNums)
		suite.NoError(err)
		seatsIndicators := make([]int, len(mappedRowSeats))
		for i, v := range mappedRowSeats {
			seatsIndicators[i] = v.AvailabilityIndicator
		}
		suite.Equal([]int{0, 0, 0, 0, 0, 0}, seatsIndicators, "wrong seats mapping")
	}
}

func (suite *SeatRulesTestSuite) TestCheckMappedRowForFragmentationFailed() {
	availableRowSeats, requestedRowsSeatsNums, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedFragmentedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	for rowID, requestedRowSeatsNums := range requestedRowsSeatsNums {
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeatsNums)
		suite.NoError(err)
		err = checkMappedRowForFragmentation(mappedRowSeats)
		suite.ErrorContains(err, "seating plan fragmentation detected")
	}
}

func (suite *SeatRulesTestSuite) TestCheckMappedRowForFragmentationSuccessful() {
	availableRowSeats, requestedRowsSeatsNums, err := getRowSeatsForSeatingPlanAndRequestedSeats(suite.ctx, suite.service.storage, &domain.IDs{OrgID: suite.org.ID}, suite.requestedSeatsGroupedByRowsGroupedBySpl)
	suite.NoError(err)
	for rowID, requestedRowSeatsNums := range requestedRowsSeatsNums {
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeatsNums)
		suite.NoError(err)
		err = checkMappedRowForFragmentation(mappedRowSeats)
		suite.NoError(err)
	}
}

func TestSeatRulesTestSuite(t *testing.T) {
	suite.Run(t, new(SeatRulesTestSuite))
}
