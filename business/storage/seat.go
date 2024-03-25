package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/proj/business/domain"
	"github.com/proj/foundation/idgen"
	"go.opentelemetry.io/otel/attribute"
)

// CreateSeats creates seats
func (s *Storage) CreateSeats(ctx context.Context, ids *domain.IDs, seats []*domain.NewSeat, t time.Time) error {
	ctx, span := s.tracer.Start(ctx, "storage.CreateSeats")
	span.SetAttributes(
		attribute.Key("ids").String(spew.Sdump(ids)),
		attribute.Key("seats").String(spew.Sdump(seats)),
	)
	defer span.End()

	err := s.execTx(ctx, func(tx *Queries) error {
		for _, seat := range seats {
			ids.ID = seat.ID
			insertSeatParams := seatToCreateParams(ids, seat, t)
			if _, err := tx.InsertSeat(ctx, insertSeatParams); err != nil {
				return err
			}

			seatLogParams := CreateSeatLogParams{
				ID:            idgen.New("sl"),
				SeatID:        seat.ID,
				UserID:        nullString(ids.UserID),
				SeatingPlanID: nullString(seat.SeatingPlanID),
				Action:        nullString("created"),
				CreatedAt:     t,
			}

			if _, err := tx.CreateSeatLog(ctx, seatLogParams); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("create seats tx: %w", err)
	}

	return nil
}

// GetSeatByID returns seat by id
func (s *Storage) GetSeatByID(ctx context.Context, ids *domain.IDs) (*domain.Seat, error) {
	getSeatParams := GetSeatByIDParams{ID: ids.ID, OrgID: ids.OrgID}
	row, err := s.queries.GetSeatByID(ctx, getSeatParams)
	if err != nil {
		return nil, fmt.Errorf("query seat: %w", err)
	}
	seat := convertToDomainSeats(row)
	return &seat, nil
}

// UpdateSeats updates seats
func (s *Storage) UpdateSeats(ctx context.Context, ids *domain.IDs, seats []*domain.UpdateSeat, t time.Time) error {
	ctx, span := s.tracer.Start(ctx, "storage.UpdateSeats")
	span.SetAttributes(
		attribute.Key("ids").String(spew.Sdump(ids)),
		attribute.Key("seats").String(spew.Sdump(seats)),
	)
	defer span.End()

	err := updateSeats(ctx, s.queries, seats, ids, t)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("update seats tx: %w", err)
	}

	return nil
}

func updateSeats(ctx context.Context, tx *Queries, seats []*domain.UpdateSeat, ids *domain.IDs, t time.Time) error {
	for _, seat := range seats {
		updateSeatParams := seatToUpdateParams(ids, seat, t)
		if err := tx.UpdateSeat(ctx, updateSeatParams); err != nil {
			return err
		}

		seatLogParams := CreateSeatLogParams{
			ID:        idgen.New("sl"),
			SeatID:    seat.ID,
			UserID:    nullString(ids.UserID),
			Action:    nullString("updated"),
			CreatedAt: t,
		}
		if _, err := tx.CreateSeatLog(ctx, seatLogParams); err != nil {
			return err
		}
	}
	return nil
}

func updateSeatsStatus(ctx context.Context, tx *Queries, ids domain.IDs, status domain.UpdateSeatStatus) error {
	var sIDs []string
	var err error
	upArgs := UpdateSeatStatusByIDsParams{
		OrderItemIds: status.OrderItemIDs,
		SeatIds:      status.SeatIDs,
		OrgID:        ids.OrgID,
		UpdatedAt:    nullTime(status.UpdatedAt),
		UpdatedByID:  nullString(ids.UserID),
		StatusCode:   int32(status.StatusCode),
	}
	sIDs, err = tx.UpdateSeatStatusByIDs(ctx, upArgs)
	if err != nil {
		return err
	}
	if status.AddOrderID != nil {
		upsoArgs := AddOrderToSeatsParams{
			SeatIds:     status.SeatIDs,
			OrderID:     nullString(*status.AddOrderID),
			OrderItemID: nullString(*status.AddOrderItemID),
			OrgID:       ids.OrgID,
			UpdatedAt:   nullTime(status.UpdatedAt),
			UpdatedByID: nullString(ids.UserID),
		}
		err = tx.AddOrderToSeats(ctx, upsoArgs)
		if err != nil {
			return err
		}
	}
	if status.RemoveOrderID {
		upsoArgs := RemoveOrderFromSeatsParams{
			SeatIds:            status.SeatIDs,
			OrderItemIds:       status.OrderItemIDs,
			OrgID:              ids.OrgID,
			UpdatedAt:          nullTime(status.UpdatedAt),
			UpdatedByID:        nullString(ids.UserID),
			RemoveOrderID:      status.RemoveOrderID,
			RemoveSplitOrderID: status.RemoveSplitOrderID,
		}
		err = tx.RemoveOrderFromSeats(ctx, upsoArgs)
		if err != nil {
			return err
		}
	}
	cSeatIDs := status.SeatIDs
	if len(status.SeatIDs) < 1 {
		cSeatIDs = sIDs
	}

	actionMsg := fmt.Sprintf("status set to %d", status.StatusCode)
	if status.RemoveOrderID {
		actionMsg += ", order removed"
	}
	if status.RemoveSplitOrderID {
		actionMsg += ", split order removed"
	}
	for _, seatID := range cSeatIDs {
		seatLogParams := CreateSeatLogParams{
			ID:        idgen.New("sl"),
			SeatID:    seatID,
			UserID:    nullString(ids.UserID),
			Action:    nullString(actionMsg),
			CreatedAt: status.UpdatedAt,
		}
		if _, err := tx.CreateSeatLog(ctx, seatLogParams); err != nil {
			return err
		}
	}
	return nil
}

// DeleteSeats deletes seats
func (s *Storage) DeleteSeats(ctx context.Context, ids *domain.IDs, seatIDs []string, t time.Time) error {
	ctx, span := s.tracer.Start(ctx, "storage.DeleteSeats")
	span.SetAttributes(
		attribute.Key("ids").String(spew.Sdump(ids)),
		attribute.Key("seatIDs").String(spew.Sdump(seatIDs)),
	)
	defer span.End()

	err := s.execTx(ctx, func(tx *Queries) error {
		deleteSeatParams := DeleteSeatsParams{
			Ids:         seatIDs,
			OrgID:       ids.OrgID,
			DeletedAt:   t,
			DeletedByID: ids.UserID,
		}
		if err := tx.DeleteSeats(ctx, deleteSeatParams); err != nil {
			return err
		}

		for _, seatID := range seatIDs {
			seatLogParams := CreateSeatLogParams{
				ID:        idgen.New("sl"),
				SeatID:    seatID,
				UserID:    nullString(ids.UserID),
				Action:    nullString("deleted"),
				CreatedAt: t,
			}
			if _, err := tx.CreateSeatLog(ctx, seatLogParams); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("delete seats tx: %w", err)
	}

	return err
}

// GetSeats returns seats
func (s *Storage) GetSeats(ctx context.Context, filter *domain.SeatsFilter, limit, offset int64) ([]domain.Seat, error) {
	getSeatsParams := GetSeatsParams{}
	if filter.Query != nil {
		getSeatsParams.IsQuery = true
		getSeatsParams.Query = *filter.Query
	}
	if filter.OrgID != nil {
		getSeatsParams.IsOrgID = true
		getSeatsParams.OrgID = *filter.OrgID
	}
	if filter.SeatingPlanID != nil {
		getSeatsParams.IsSeatingPlanID = true
		getSeatsParams.SeatingPlanID = *filter.SeatingPlanID
	}
	if filter.IDs != nil {
		getSeatsParams.IsIds = true
		getSeatsParams.Ids = append(getSeatsParams.Ids, filter.IDs...)
	}
	if filter.OrderBy != nil {
		if filter.Desc {
			switch *filter.OrderBy {
			case "name":
				getSeatsParams.OrderByNameDesc = true
			case "num":
				getSeatsParams.OrderByNumDesc = true
			default:
				getSeatsParams.OrderByNameDesc = true
			}
		} else {
			switch *filter.OrderBy {
			case "name":
				getSeatsParams.OrderByNameAsc = true
			case "num":
				getSeatsParams.OrderByNumAsc = true
			default:
				getSeatsParams.OrderByNameAsc = true
			}
		}
	}
	getSeatsParams.Limit = limit
	getSeatsParams.Offset = offset

	rows, err := s.queries.GetSeats(ctx, getSeatsParams)
	if err != nil {
		return nil, fmt.Errorf("query seats: %w", err)
	}
	pns := make([]domain.Seat, 0, len(rows))
	for _, pn := range rows {
		pns = append(pns, convertToDomainSeats(pn))
	}

	return pns, nil
}

// ClearOfferedExpiredSeats clears offered seats that are expired
func (s *Storage) ClearOfferedExpiredSeats(ctx context.Context) error {
	return s.queries.ClearOfferedExpiredSeats(ctx)
}

// GetSeatsBySeatingPlanID returns seats by seating plan id
func (s *Storage) GetSeatsBySeatingPlanID(ctx context.Context, ids *domain.IDs) ([]domain.Seat, error) {
	rows, err := s.queries.GetSeatsBySeatingPlanID(ctx, GetSeatsBySeatingPlanIDParams{
		OrgID:         ids.OrgID,
		SeatingPlanID: ids.SplID,
	})
	if err != nil {
		return nil, fmt.Errorf("query seats: %w", err)
	}
	pns := make([]domain.Seat, 0, len(rows))
	for _, pn := range rows {
		pns = append(pns, convertToDomainSeats(pn))
	}

	return pns, nil
}

// GetSeatsWithSeatGroups returns seats with seat groups
func (s *Storage) GetSeatsWithSeatGroups(ctx context.Context, ids *domain.IDs, seatIDs []string) ([]domain.SeatWithSeatGroup, error) {
	rows, err := s.queries.GetSeatsWithSeatGroups(ctx, GetSeatsWithSeatGroupsParams{
		OrgID:         ids.OrgID,
		SeatingPlanID: ids.SplID,
		Ids:           seatIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("query seats with seat groups: %w", err)
	}
	seatsWithSeatGroup := make([]domain.SeatWithSeatGroup, len(rows))
	for i, row := range rows {
		seatsWithSeatGroup[i] = convertSeatWithSGRowToDomainSeatWithSG(row)
	}
	return seatsWithSeatGroup, nil
}

func seatToUpdateParams(ids *domain.IDs, seat *domain.UpdateSeat, t time.Time) UpdateSeatParams {
	updateSeatParams := UpdateSeatParams{
		ID:          seat.ID,
		OrgID:       ids.OrgID,
		UpdatedAt:   t,
		UpdatedByID: ids.UserID,
	}
	if seat.Num != nil {
		updateSeatParams.Num = *seat.Num
		updateSeatParams.SetNum = true
	}
	if seat.SvgTransform != nil {
		updateSeatParams.SvgTransform = *seat.SvgTransform
		updateSeatParams.SetSvgTransform = true
	}
	if seat.SvgPath != nil {
		updateSeatParams.SvgPath = *seat.SvgPath
		updateSeatParams.SetSvgPath = true
	}
	if seat.Name != nil {
		updateSeatParams.Name = *seat.Name
		updateSeatParams.SetName = true
	}
	if seat.X != nil {
		updateSeatParams.X = *seat.X
		updateSeatParams.SetX = true
	}
	if seat.Y != nil {
		updateSeatParams.Y = *seat.Y
		updateSeatParams.SetY = true
	}
	if seat.PriceCategoryID != nil {
		if *seat.PriceCategoryID == "" {
			updateSeatParams.SetPriceCategoryIDToNull = true
		} else {
			updateSeatParams.PriceCategoryID = *seat.PriceCategoryID
			updateSeatParams.SetPriceCategoryID = true
		}
	}
	if seat.StatusCode != nil {
		updateSeatParams.StatusCode = *seat.StatusCode
		updateSeatParams.SetStatusCode = true
	}
	if seat.SplitOrderID != nil {
		updateSeatParams.SplitOrderID = *seat.SplitOrderID
		updateSeatParams.SetSplitOrderID = true
	}
	if seat.Wheelchair != nil {
		updateSeatParams.Wheelchair = *seat.Wheelchair
		updateSeatParams.SetWheelchair = true
	}
	if seat.OverrideNum != nil {
		updateSeatParams.OverrideNum = *seat.OverrideNum
		updateSeatParams.SetOverrideNum = true
	}
	if seat.LinkedSeatID != nil {
		updateSeatParams.LinkedSeatID = *seat.LinkedSeatID
		updateSeatParams.SetLinkedSeatID = true
		if *seat.LinkedSeatID == "" {
			updateSeatParams.StatusCode = int32(domain.SeatStatusAvailable)
		} else {
			updateSeatParams.StatusCode = int32(domain.SeatStatusLocked)
		}
		updateSeatParams.SetStatusCode = true
	}
	if seat.Description != nil {
		updateSeatParams.Description = *seat.Description
		updateSeatParams.SetDescription = true
	}
	if seat.BestSeatGroupID != nil {
		updateSeatParams.BestSeatGroupID = *seat.BestSeatGroupID
		updateSeatParams.SetBestSeatGroupID = true
	}
	if seat.SeatRowID != nil {
		updateSeatParams.SeatRowID = *seat.SeatRowID
		updateSeatParams.SetSeatRowID = true
	}
	if seat.SeatBlockID != nil {
		updateSeatParams.SeatBlockID = *seat.SeatBlockID
		updateSeatParams.SetSeatBlockID = true
	}
	if seat.SeatGateID != nil {
		updateSeatParams.SeatGateID = *seat.SeatGateID
		updateSeatParams.SetSeatGateID = true
	}
	if seat.OrderID != nil {
		updateSeatParams.OrderID = *seat.OrderID
		updateSeatParams.SetOrderID = true
	}
	if seat.OrderItemID != nil {
		updateSeatParams.OrderItemID = *seat.OrderItemID
		updateSeatParams.SetOrderItemID = true
	}
	if seat.OfferedAt != nil {
		updateSeatParams.OfferedAt = *seat.OfferedAt
		updateSeatParams.SetOfferedAt = true
	}
	return updateSeatParams
}

func seatToCreateParams(ids *domain.IDs, seat *domain.NewSeat, t time.Time) InsertSeatParams {
	if (seat.StatusCode == nil) || (*seat.StatusCode == 0) {
		seat.StatusCode = nullInt32P(int32(domain.SeatStatusAvailable))
	}
	insertSeatParams := InsertSeatParams{
		ID:              ids.ID,
		OrgID:           ids.OrgID,
		SeatingPlanID:   ids.SplID,
		Num:             nullPInt32(&seat.Num),
		X:               nullInt32(seat.X),
		Y:               nullInt32(seat.Y),
		Name:            nullPString(seat.Name),
		PriceCategoryID: nullPID(seat.PriceCategoryID),
		SeatBlockID:     nullPString(seat.SeatBlockID),
		SeatGateID:      nullPString(seat.SeatGateID),
		SeatRowID:       nullPString(seat.SeatRowID),
		BestSeatGroupID: nullPString(seat.BestSeatGroupID),
		StatusCode:      nullPInt32(seat.StatusCode),
		Wheelchair:      nullPBool(seat.Wheelchair),
		OverrideNum:     nullPString(seat.OverrideNum),
		LinkedSeatID:    nullPString(seat.LinkedSeatID),
		Description:     nullPString(seat.Description),
		SvgPath:         nullPString(seat.SvgPath),
		SvgTransform:    nullPString(seat.SvgTransform),
		CreatedAt:       t,
		CreatedByID:     ids.UserID,
	}

	return insertSeatParams
}

func convertToDomainSeats(seatRow Seat) domain.Seat {
	seat := domain.Seat{
		ID:              seatRow.ID,
		OrgID:           seatRow.OrgID,
		Num:             seatRow.Num.Int32,
		Name:            seatRow.Name.String,
		X:               seatRow.X.Int32,
		Y:               seatRow.Y.Int32,
		OrderID:         seatRow.OrderID.String,
		PriceCategoryID: &seatRow.PriceCategoryID.String,
		StatusCode:      domain.SeatStatus(seatRow.StatusCode.Int32),
		SeatingPlanID:   seatRow.SeatingPlanID,
		OfferedAt:       seatRow.OfferedAt.Time,
		SvgTransform:    validStr(seatRow.SvgTransform),
		SvgPath:         validStr(seatRow.SvgPath),
		Wheelchair:      validBool(seatRow.Wheelchair),
		OverrideNum:     validStrP(seatRow.OverrideNum),
		LinkedSeatID:    validStr(seatRow.LinkedSeatID),
		Description:     validStr(seatRow.Description),
		SeatRowID:       validStrP(seatRow.SeatRowID),
		SeatBlockID:     validStrP(seatRow.SeatBlockID),
		SeatGateID:      validStrP(seatRow.SeatGateID),
		BestSeatGroupID: validStrP(seatRow.BestSeatGroupID),
		Modifiers: domain.Modifiers{
			CreatedAt:   seatRow.CreatedAt,
			CreatedByID: seatRow.CreatedByID,
			UpdatedAt:   validTimeP(seatRow.UpdatedAt),
			UpdatedByID: validStrP(seatRow.UpdatedByID),
			DeletedAt:   validTimeP(seatRow.DeletedAt),
			DeletedByID: validStrP(seatRow.DeletedByID),
		},
	}
	return seat
}

// CreateSeatLog creates a new seat log
func (s *Storage) CreateSeatLog(ctx context.Context, ids *domain.IDs, sg *domain.SeatLog, t time.Time) error {
	insertSeatLogParams := seatLogToCreateParams(ids, sg, t)

	_, err := s.queries.CreateSeatLog(ctx, insertSeatLogParams)
	if err != nil {
		return err
	}

	return err
}

// GetSeatLogs returns a seat log
func (s *Storage) GetSeatLogs(ctx context.Context, limit, offset int64) ([]*domain.SeatLog, error) {
	getSeatLogsParams := GetSeatLogsParams{}
	getSeatLogsParams.Limit = limit
	getSeatLogsParams.Offset = offset

	rows, err := s.queries.GetSeatLogs(ctx, getSeatLogsParams)
	if err != nil {
		return nil, fmt.Errorf("query seat logs: %w", err)
	}
	pns := make([]*domain.SeatLog, 0, len(rows))
	for _, pn := range rows {
		pns = append(pns, convertSeatLogsRowToDomainSeatLogs(pn))
	}

	return pns, nil
}

// GetBestSeatsByPosition returns the best seats by position
func (s *Storage) GetBestSeatsByPosition(ctx context.Context, splID, priceCategoryID string, refPointX, refPointY int32) ([]domain.BestSeat, error) {
	statusCode := int32(domain.SeatStatusAvailable)
	var limit int32 = 1000
	reqParams := getBestSeatsByPositionParams(splID, priceCategoryID, statusCode, refPointX, refPointY, limit)
	rows, err := s.queries.GetBestSeatsByPosition(ctx, reqParams)
	if err != nil {
		return nil, fmt.Errorf("query qty of best seats per price: %w", err)
	}
	res := make([]domain.BestSeat, len(rows))
	for i, bs := range rows {
		res[i] = convertToDomainBestSeat(bs)
	}
	return res, nil
}

// GetRowSeatsBySeatingPlanID returns the seats for the rows in the given seatingPlanID
func (s *Storage) GetRowSeatsBySeatingPlanID(ctx context.Context, splID, orgID, rowID string) ([]domain.Seat, error) {
	statusCode := int32(domain.SeatStatusAvailable)
	reqParams := GetRowSeatsBySeatingPlanIDParams{SeatingPlanID: splID, OrgID: orgID, StatusCode: statusCode, RowID: rowID}
	rows, err := s.queries.GetRowSeatsBySeatingPlanID(ctx, reqParams)
	if err != nil {
		return nil, fmt.Errorf("query qty of best seats per price: %w", err)
	}
	res := make([]domain.Seat, len(rows))
	for i, rs := range rows {
		res[i] = convertToDomainSeat(rs)
	}
	return res, nil
}

func (s *Storage) GetSeatsCountGroupedByPriceCategories(ctx context.Context, splID, orgID string, onlyAvailableSeats bool) ([]domain.SeatsPerPriceCategories, error) {
	statusCode := int32(domain.SeatStatusAvailable)
	reqParams := GetSeatsCountGroupedByPriceCategoriesParams{OrgID: orgID, SeatingPlanID: splID, StatusCode: statusCode, OnlyAvailable: onlyAvailableSeats}
	rows, err := s.queries.GetSeatsCountGroupedByPriceCategories(ctx, reqParams)
	if err != nil {
		return nil, fmt.Errorf("query count of seats per price categories: %w", err)
	}
	res := make([]domain.SeatsPerPriceCategories, len(rows))
	for i, rs := range rows {
		res[i] = convertToSeatsPerPriceCategories(rs)
	}
	return res, nil
}

func convertToSeatsPerPriceCategories(r GetSeatsCountGroupedByPriceCategoriesRow) domain.SeatsPerPriceCategories {
	dRes := domain.SeatsPerPriceCategories{Count: r.Count}
	if r.PriceCategoryID.Valid {
		dRes.PriceCategoryID = &r.PriceCategoryID.String
	}
	return dRes
}

func convertSeatLogsRowToDomainSeatLogs(s SeatLog) *domain.SeatLog {
	seatLog := &domain.SeatLog{
		ID:            s.ID,
		SeatID:        s.SeatID,
		Action:        s.Action.String,
		OrderID:       s.OrderID.String,
		SeatingPlanID: s.SeatingPlanID.String,
	}

	return seatLog
}

func seatLogToCreateParams(ids *domain.IDs, log *domain.SeatLog, t time.Time) CreateSeatLogParams {
	createSeatLogParams := CreateSeatLogParams{
		ID:            ids.ID,
		OrderID:       nullString(log.OrderID),
		Action:        nullString(log.Action),
		SeatID:        log.SeatID,
		SeatingPlanID: nullString(log.SeatingPlanID),
		CreatedAt:     t,
	}

	return createSeatLogParams
}

func convertSeatWithSGRowToDomainSeatWithSG(s GetSeatsWithSeatGroupsRow) domain.SeatWithSeatGroup {
	sWithSg := domain.SeatWithSeatGroup{
		ID:              s.ID,
		SeatName:        validStr(s.Name),
		SeatNum:         validInt(s.Num),
		SeatOverrideNum: validStrP(s.OverrideNum),
		//---
		RowName: validStr(s.RowName),
		RowNum:  validStr(s.RowNum),
		//---
		BlockName: validStrP(s.BlockName),
		BlockNum:  validStrP(s.BlockNum),
		//---
		GateName: validStrP(s.GateName),
		GateNum:  validStrP(s.GateNum),
	}
	return sWithSg
}

func getBestSeatsByPositionParams(splID, priceCategoryID string, statusCode, refPointX, refPointY, limit int32) GetBestSeatsByPositionParams {
	params := GetBestSeatsByPositionParams{
		SeatingPlanID:   splID,
		PriceCategoryID: nullString(priceCategoryID),
		StatusCode:      nullInt32(statusCode),
		RefPointX:       refPointX,
		RefPointY:       refPointY,
		Limit:           limit,
	}
	return params
}

func convertToDomainBestSeat(s GetBestSeatsByPositionRow) domain.BestSeat {
	bs := domain.BestSeat{SeatID: s.SeatID, SeatNum: int(s.Num.Int32), SeatRowID: s.RowID}
	return bs
}

func convertToDomainSeat(s GetRowSeatsBySeatingPlanIDRow) domain.Seat {
	rs := domain.Seat{ID: s.SeatID, Num: s.Num.Int32, SeatRowID: &s.RowID}
	return rs
}
