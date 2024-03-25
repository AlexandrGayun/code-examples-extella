package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/proj/business/domain"
	"github.com/proj/business/storage"
)

func (s *Service) ValidateSeatRules(ctx context.Context, orgID string, splIDBookAllSeatsInGroup map[string]string, requestedSeatsGroupedByRowsGroupedBySpl map[string]map[string][]domain.Seat) error {
	ids := &domain.IDs{
		OrgID: orgID,
	}
	availableRowSeats, requestedRowsSeats, err := getRowSeatsForSeatingPlanAndRequestedSeats(ctx, s.storage, ids, requestedSeatsGroupedByRowsGroupedBySpl)
	if err != nil {
		return err
	}
	availableSeatsByPriceCategoriesPerSpl, err := getSeatsWithPriceCategoriesForSpl(ctx, s.storage, ids, requestedSeatsGroupedByRowsGroupedBySpl, true)
	if err != nil {
		return err
	}
	allSeatsByPriceCategoriesPerSpl, err := getSeatsWithPriceCategoriesForSpl(ctx, s.storage, ids, requestedSeatsGroupedByRowsGroupedBySpl, false)
	if err != nil {
		return err
	}
	for rowID, requestedRowSeats := range requestedRowsSeats {
		err = checkEventFullGroupOrderingRestriction(rowID, availableRowSeats[rowID], requestedRowSeats, splIDBookAllSeatsInGroup, requestedSeatsGroupedByRowsGroupedBySpl)
		if err != nil {
			return err
		}
		if skipFragmentationCheck(requestedRowSeats, availableSeatsByPriceCategoriesPerSpl, allSeatsByPriceCategoriesPerSpl) {
			continue
		}
		mappedRowSeats, err := mapAllRowSeatsForAvailability(availableRowSeats[rowID], requestedRowSeats)
		if err != nil {
			return err
		}
		err = checkMappedRowForFragmentation(mappedRowSeats)
		if err != nil {
			return err
		}
	}
	return nil
}

func getRowSeatsForSeatingPlanAndRequestedSeats(ctx context.Context, s *storage.Storage, ids *domain.IDs, requestedSeatsGroupedByRowsGroupedBySpl map[string]map[string][]domain.Seat) (availableRowSeats, requestedRowSeats map[string][]domain.Seat, err error) {
	requestedRowSeats = make(map[string][]domain.Seat)
	availableRowSeats = make(map[string][]domain.Seat)
	for splID, rows := range requestedSeatsGroupedByRowsGroupedBySpl {
		for rowID := range rows {
			seatsForRow, err := s.GetRowSeatsBySeatingPlanID(ctx, splID, ids.OrgID, rowID)
			if err != nil {
				return nil, nil, fmt.Errorf("error while querying seats %w", err)
			}
			availableRowSeats[rowID] = seatsForRow
			requestedRowSeats[rowID] = requestedSeatsGroupedByRowsGroupedBySpl[splID][rowID]
		}
	}
	return availableRowSeats, requestedRowSeats, nil
}

func getSeatsWithPriceCategoriesForSpl(ctx context.Context, s *storage.Storage, ids *domain.IDs, requestedSeatsGroupedByRowsGroupedBySpl map[string]map[string][]domain.Seat, onlyAvailable bool) (map[string][]domain.SeatsPerPriceCategories, error) {
	seatsByPriceCategoriesPerSpl := make(map[string][]domain.SeatsPerPriceCategories)
	for splID := range requestedSeatsGroupedByRowsGroupedBySpl {
		res, err := s.GetSeatsCountGroupedByPriceCategories(ctx, splID, ids.OrgID, onlyAvailable)
		if err != nil {
			return nil, fmt.Errorf("error while querying seats price categories %w", err)
		}
		seatsByPriceCategoriesPerSpl[splID] = res
	}
	return seatsByPriceCategoriesPerSpl, nil
}

func skipFragmentationCheck(requestedRowSeats []domain.Seat, availableSeatsByPriceCategoriesPerSpl, allSeatsByPriceCategoriesPerSpl map[string][]domain.SeatsPerPriceCategories) bool {
	currentRowPriceCategoriesSeatsCount := make(map[string]int64)
	for _, v := range requestedRowSeats {
		if v.PriceCategoryID != nil {
			currentRowPriceCategoriesSeatsCount[*v.PriceCategoryID]++
		}
	}
	for _, pcIDsArr := range availableSeatsByPriceCategoriesPerSpl {
		for _, v := range pcIDsArr {
			if v.PriceCategoryID != nil {
				count, ok := currentRowPriceCategoriesSeatsCount[*v.PriceCategoryID]
				if ok {
					allSplSeatsPerPriceCategoryCount := getCountOfAllSplPriceCategorySeats(allSeatsByPriceCategoriesPerSpl, *v.PriceCategoryID)
					if float32(v.Count) < float32(allSplSeatsPerPriceCategoryCount)/float32(10) {
						// if the count of requested seats with price category is lower than 10% of seating plan seats with same price category
						return true
					}
					if count == v.Count-1 {
						// if the count of requested seats with price category is equal to number of remaining available seating plan price categories seats minus 1
						return true
					}
					if count == v.Count {
						// if the count of requested seats with price category is equal to remaining available seating plan price categories seats
						return true
					}
				}
			}
		}
	}
	return false
}

func getCountOfAllSplPriceCategorySeats(allSeatsByPriceCategoriesPerSpl map[string][]domain.SeatsPerPriceCategories, pcID string) int64 {
	for _, pcIDsArr := range allSeatsByPriceCategoriesPerSpl {
		for _, v := range pcIDsArr {
			if v.PriceCategoryID != nil && *v.PriceCategoryID == pcID {
				return v.Count
			}
		}
	}
	return 0
}

func checkEventFullGroupOrderingRestriction(rowID string, availableSeatsByRowID, requestedRowSeats []domain.Seat, splIDBookAllSeatsInGroup map[string]string, requestedSeatsGroupedByRowsGroupedBySpl map[string]map[string][]domain.Seat) error {
	if len(requestedRowSeats) > len(availableSeatsByRowID) {
		return errors.New("requested amount of tickets exceed available seats")
	}
	for splID, eventTitle := range splIDBookAllSeatsInGroup {
		for checkableRow := range requestedSeatsGroupedByRowsGroupedBySpl[splID] {
			if rowID == checkableRow { // means that rule is applied to this row
				if len(availableSeatsByRowID) != len(requestedRowSeats) {
					return fmt.Errorf("violate event restriction for event %v, only full group seats ordering allowed", eventTitle)
				}
			}
		}
	}
	return nil
}

// we map all row seats from first to last to array marking available/unavailable seats indexes with according values(1/0)
// than we will traverse this array to check if seats row will be fragmented or not after all requested seats will be marked
func mapAllRowSeatsForAvailability(availableRowSeats, requestedRowSeats []domain.Seat) ([]domain.MappedRowSeat, error) {
	allRowSeats := make([]domain.MappedRowSeat, availableRowSeats[len(availableRowSeats)-1].Num-availableRowSeats[0].Num+1)
	firstSeatNumInRow := availableRowSeats[0].Num
	for i := range allRowSeats {
		curSeatNum := firstSeatNumInRow + int32(i)
		index := indexOfRowSeat(availableRowSeats, curSeatNum)
		if index != -1 {
			allRowSeats[i] = domain.MappedRowSeat{AvailabilityIndicator: 1, Seat: &availableRowSeats[index]} // means the seat is available
		} else {
			allRowSeats[i] = domain.MappedRowSeat{AvailabilityIndicator: 0} // means the seat is unavailable for some reason(ordered/already booked/unavailable/etc)
		}
	}
	for _, requestedSeat := range requestedRowSeats {
		index := indexOfRowSeat(availableRowSeats, requestedSeat.Num)
		if index == -1 {
			return nil, fmt.Errorf("something went wrong. requested seat num %v is unavailable", requestedSeat.Num)
		}
		allRowSeats[requestedSeat.Num-firstSeatNumInRow].AvailabilityIndicator = 0
		allRowSeats[requestedSeat.Num-firstSeatNumInRow].RequestedForNow = true
	}
	return allRowSeats, nil
}

func checkMappedRowForFragmentation(mappedRowSeats []domain.MappedRowSeat) error {
	for i, mappedSeat := range mappedRowSeats {
		if mappedSeat.AvailabilityIndicator == 0 { // check only requested seats
			err := fragmentationCheck(i, mappedRowSeats)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// walk through mapped array step by step successively left, than right and detect fragmentation
func fragmentationCheck(i int, rowSeats []domain.MappedRowSeat) error {
	movesCount := 2
	var leftSpaces int32
	var rightSpaces int32

	// moving left until find rowSeats with value != 0
	// than detect if the gap is large enough
	startCountingFreeSpace := false
	for j := 1; j < len(rowSeats); j++ {
		if i-j < 0 || movesCount == 0 {
			break
		}
		if rowSeats[i-j].AvailabilityIndicator != 0 {
			startCountingFreeSpace = true
			leftSpaces++
			movesCount--
		} else if startCountingFreeSpace {
			movesCount--
		}
	}
	if startCountingFreeSpace && leftSpaces != 2 {
		for k := i; k > 0; k-- {
			if rowSeats[k].RequestedForNow && rowSeats[k-1].RequestedForNow {
				return fragmentationError(&rowSeats[k].Seat.ID)
			}
		}
		return fragmentationError(nil)
	}
	// the same approach as for the left side
	movesCount = 2
	startCountingFreeSpace = false
	for j := 1; j < len(rowSeats); j++ {
		if i+j > len(rowSeats)-1 || movesCount == 0 {
			break
		}
		if rowSeats[i+j].AvailabilityIndicator != 0 {
			startCountingFreeSpace = true
			rightSpaces++
			movesCount--
		} else if startCountingFreeSpace {
			movesCount--
		}
	}
	if startCountingFreeSpace && rightSpaces != 2 {
		for k := i; k < len(rowSeats)-1; k++ {
			if rowSeats[k].RequestedForNow && !rowSeats[k+1].RequestedForNow {
				return fragmentationError(&rowSeats[k].Seat.ID)
			}
		}
		return fragmentationError(nil)
	}

	return nil
}

func indexOfRowSeat(rowSeats []domain.Seat, num int32) int {
	for i, x := range rowSeats {
		if num == x.Num {
			return i
		}
	}
	return -1
}

func fragmentationError(conflictingSeatID *string) *domain.SeatRuleFragmentationError {
	ruleErr := &domain.SeatRuleFragmentationError{
		OrigError: errors.New("seating plan fragmentation detected"),
	}
	if conflictingSeatID != nil {
		ruleErr.ConflictingSeatID = *conflictingSeatID
	}
	return ruleErr
}
