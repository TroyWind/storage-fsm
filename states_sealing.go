package sealing

import (
	"bytes"
	"context"
	"fmt"
	"github.com/filecoin-project/storage-fsm/lib/dlog/dsfsmlog"
	"go.uber.org/zap"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-statemachine"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-storage/storage"
)

var DealSectorPriority = 1024

func (m *Sealing) handlePacking(ctx statemachine.Context, sector SectorInfo) error {
	log.Infow("performing filling up rest of the sector...", "sector", sector.SectorNumber)
	dsfsmlog.L.Debug("handlePacking", zap.Int("piece len", len(sector.Pieces)))
	for _, p := range sector.pieceInfos() {
		dsfsmlog.L.Debug("handlePacking piece info", zap.Uint64("piece size", uint64(p.Size)))
	}
	var allocated abi.UnpaddedPieceSize
	for _, piece := range sector.Pieces {
		allocated += piece.Piece.Size.Unpadded()
	}

	ubytes := abi.PaddedPieceSize(m.sealer.SectorSize()).Unpadded()

	if allocated > ubytes {
		return xerrors.Errorf("too much data in sector: %d > %d", allocated, ubytes)
	}

	fillerSizes, err := fillersFromRem(ubytes - allocated)
	if err != nil {
		return err
	}

	if len(fillerSizes) > 0 {
		dsfsmlog.L.Debug(fmt.Sprintf("Creating %d filler pieces for sector %d", len(fillerSizes), sector.SectorNumber))
		log.Warnf("Creating %d filler pieces for sector %d", len(fillerSizes), sector.SectorNumber)
	}

	fillerPieces, err := m.pledgeSector(ctx.Context(), m.minerSector(sector.SectorNumber), sector.existingPieceSizes(), fillerSizes...)
	if err != nil {
		return xerrors.Errorf("filling up the sector (%v): %w", fillerSizes, err)
	}

	return ctx.Send(SectorPacked{FillerPieces: fillerPieces})
}

func (m *Sealing) getTicket(ctx statemachine.Context, sector SectorInfo) (abi.SealRandomness, abi.ChainEpoch, error) {
	tok, epoch, err := m.api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handlePreCommit1: api error, not proceeding: %+v", err)
		return nil, 0, nil
	}

	ticketEpoch := epoch - SealRandomnessLookback
	buf := new(bytes.Buffer)
	if err := m.maddr.MarshalCBOR(buf); err != nil {
		return nil, 0, err
	}

	pci, err := m.api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		return nil, 0, xerrors.Errorf("getting precommit info: %w", err)
	}

	if pci != nil {
		ticketEpoch = pci.Info.SealRandEpoch
	}

	rand, err := m.api.ChainGetRandomness(ctx.Context(), tok, crypto.DomainSeparationTag_SealRandomness, ticketEpoch, buf.Bytes())
	if err != nil {
		return nil, 0, err
	}

	return abi.SealRandomness(rand), ticketEpoch, nil
}

func (m *Sealing) handlePreCommit1(ctx statemachine.Context, sector SectorInfo) error {
	startAt := time.Now()
	dsfsmlog.L.Debug("handlePreCommit1")
	if err := checkPieces(ctx.Context(), sector, m.api); err != nil { // Sanity check state
		switch err.(type) {
		case *ErrApi:
			log.Errorf("handlePreCommit1: api error, not proceeding: %+v", err)
			return nil
		case *ErrInvalidDeals:
			return ctx.Send(SectorPackingFailed{xerrors.Errorf("invalid dealIDs in sector: %w", err)})
		case *ErrExpiredDeals: // Probably not much we can do here, maybe re-pack the sector?
			return ctx.Send(SectorPackingFailed{xerrors.Errorf("expired dealIDs in sector: %w", err)})
		default:
			return xerrors.Errorf("checkPieces sanity check error: %w", err)
		}
	}

	log.Infow("performing sector replication...", "sector", sector.SectorNumber)
	ticketValue, ticketEpoch, err := m.getTicket(ctx, sector)
	if err != nil {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("getting ticket failed: %w", err)})
	}

	pc1o, err := m.sealer.SealPreCommit1(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorNumber), ticketValue, sector.pieceInfos())
	if err != nil {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("seal pre commit(1) failed: %w", err)})
	}

	dsfsmlog.L.Debug("handlePreCommit1", zap.String("use time", time.Now().Sub(startAt).String()))
	return ctx.Send(SectorPreCommit1{
		PreCommit1Out: pc1o,
		TicketValue:   ticketValue,
		TicketEpoch:   ticketEpoch,
	})
}

func (m *Sealing) handlePreCommit2(ctx statemachine.Context, sector SectorInfo) error {
	startAt := time.Now()
	dsfsmlog.L.Debug("handlePreCommit2")
	cids, err := m.sealer.SealPreCommit2(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorNumber), sector.PreCommit1Out)
	if err != nil {
		return ctx.Send(SectorSealPreCommit2Failed{xerrors.Errorf("seal pre commit(2) failed: %w", err)})
	}
	dsfsmlog.L.Debug("handlePreCommit2", zap.String("use time", time.Now().Sub(startAt).String()))

	return ctx.Send(SectorPreCommit2{
		Unsealed: cids.Unsealed,
		Sealed:   cids.Sealed,
	})
}

func (m *Sealing) handlePreCommitting(ctx statemachine.Context, sector SectorInfo) error {
	startAt := time.Now()
	dsfsmlog.L.Debug("handlePreCommitting")

	tok, height, err := m.api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	waddr, err := m.api.StateMinerWorkerAddress(ctx.Context(), m.maddr, tok)
	if err != nil {
		log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	if err := checkPrecommit(ctx.Context(), m.Address(), sector, tok, height, m.api); err != nil {
		switch err := err.(type) {
		case *ErrApi:
			log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
			return nil
		case *ErrBadCommD: // TODO: Should this just back to packing? (not really needed since handlePreCommit1 will do that too)
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("bad CommD error: %w", err)})
		case *ErrExpiredTicket:
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("ticket expired: %w", err)})
		case *ErrBadTicket:
			return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("bad ticket: %w", err)})
		case *ErrPrecommitOnChain:
			return ctx.Send(SectorPreCommitLanded{TipSet: tok}) // we re-did precommit
		default:
			return xerrors.Errorf("checkPrecommit sanity check error: %w", err)
		}
	}

	expiration, err := m.pcp.Expiration(ctx.Context(), sector.Pieces...)
	if err != nil {
		return ctx.Send(SectorSealPreCommit1Failed{xerrors.Errorf("handlePreCommitting: failed to compute pre-commit expiry: %w", err)})
	}

	params := &miner.SectorPreCommitInfo{
		Expiration:   expiration,
		SectorNumber: sector.SectorNumber,
		SealProof:    sector.SectorType,

		SealedCID:     *sector.CommR,
		SealRandEpoch: sector.TicketEpoch,
		DealIDs:       sector.dealIDs(),
	}

	enc := new(bytes.Buffer)
	if err := params.MarshalCBOR(enc); err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("could not serialize pre-commit sector parameters: %w", err)})
	}

	log.Info("submitting precommit for sector: ", sector.SectorNumber)
	mcid, err := m.api.SendMsg(ctx.Context(), waddr, m.maddr, builtin.MethodsMiner.PreCommitSector, big.NewInt(0), big.NewInt(1), 1000000, enc.Bytes())
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("pushing message to mpool: %w", err)})
	}

	dsfsmlog.L.Debug("handlePreCommitting", zap.String("use time", time.Now().Sub(startAt).String()))

	return ctx.Send(SectorPreCommitted{Message: mcid})
}

func (m *Sealing) handlePreCommitWait(ctx statemachine.Context, sector SectorInfo) error {
	startAt := time.Now()
	dsfsmlog.L.Debug("handlePreCommitWait")

	if sector.PreCommitMessage == nil {
		return ctx.Send(SectorChainPreCommitFailed{xerrors.Errorf("precommit message was nil")})
	}

	// would be ideal to just use the events.Called handler, but it wouldnt be able to handle individual message timeouts
	log.Info("Sector precommitted: ", sector.SectorNumber)
	mw, err := m.api.StateWaitMsg(ctx.Context(), *sector.PreCommitMessage)
	if err != nil {
		return ctx.Send(SectorChainPreCommitFailed{err})
	}

	if mw.Receipt.ExitCode != 0 {
		log.Error("sector precommit failed: ", mw.Receipt.ExitCode)
		err := xerrors.Errorf("sector precommit failed: %d", mw.Receipt.ExitCode)
		return ctx.Send(SectorChainPreCommitFailed{err})
	}
	log.Info("precommit message landed on chain: ", sector.SectorNumber)

	dsfsmlog.L.Debug("handlePreCommitWait", zap.String("use time", time.Now().Sub(startAt).String()))

	return ctx.Send(SectorPreCommitLanded{TipSet: mw.TipSetTok})
}

func (m *Sealing) handleWaitSeed(ctx statemachine.Context, sector SectorInfo) error {
	startAt := time.Now()
	dsfsmlog.L.Debug("handleWaitSeed")

	pci, err := m.api.StateSectorPreCommitInfo(ctx.Context(), m.maddr, sector.SectorNumber, sector.PreCommitTipSet)
	if err != nil {
		return xerrors.Errorf("getting precommit info: %w", err)
	}
	if pci == nil {
		return ctx.Send(SectorChainPreCommitFailed{error: xerrors.Errorf("precommit info not found on chain")})
	}

	randHeight := pci.PreCommitEpoch + miner.PreCommitChallengeDelay

	err = m.events.ChainAt(func(ectx context.Context, tok TipSetToken, curH abi.ChainEpoch) error {
		buf := new(bytes.Buffer)
		if err := m.maddr.MarshalCBOR(buf); err != nil {
			return err
		}
		rand, err := m.api.ChainGetRandomness(ectx, tok, crypto.DomainSeparationTag_InteractiveSealChallengeSeed, randHeight, buf.Bytes())
		if err != nil {
			err = xerrors.Errorf("failed to get randomness for computing seal proof (ch %d; rh %d; tsk %x): %w", curH, randHeight, tok, err)

			_ = ctx.Send(SectorChainPreCommitFailed{error: err})
			return err
		}

		_ = ctx.Send(SectorSeedReady{SeedValue: abi.InteractiveSealRandomness(rand), SeedEpoch: randHeight})

		dsfsmlog.L.Debug("handleWaitSeed", zap.String("use time", time.Now().Sub(startAt).String()))

		return nil
	}, func(ctx context.Context, ts TipSetToken) error {
		log.Warn("revert in interactive commit sector step")
		// TODO: need to cancel running process and restart...
		return nil
	}, InteractivePoRepConfidence, randHeight)
	if err != nil {
		log.Warn("waitForPreCommitMessage ChainAt errored: ", err)
	}

	return nil
}

func (m *Sealing) handleCommitting(ctx statemachine.Context, sector SectorInfo) error {
	startAt := time.Now()
	dsfsmlog.L.Debug("handleCommitting")

	log.Info("scheduling seal proof computation...")

	log.Infof("KOMIT %d %x(%d); %x(%d); %v; r:%x; d:%x", sector.SectorNumber, sector.TicketValue, sector.TicketEpoch, sector.SeedValue, sector.SeedEpoch, sector.pieceInfos(), sector.CommR, sector.CommD)

	cids := storage.SectorCids{
		Unsealed: *sector.CommD,
		Sealed:   *sector.CommR,
	}
	c2in, err := m.sealer.SealCommit1(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorNumber), sector.TicketValue, sector.SeedValue, sector.pieceInfos(), cids)
	if err != nil {
		return ctx.Send(SectorComputeProofFailed{xerrors.Errorf("computing seal proof failed(1): %w", err)})
	}

	proof, err := m.sealer.SealCommit2(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorNumber), c2in)
	if err != nil {
		return ctx.Send(SectorComputeProofFailed{xerrors.Errorf("computing seal proof failed(2): %w", err)})
	}

	tok, _, err := m.api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handleCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	if err := m.checkCommit(ctx.Context(), sector, proof, tok); err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("commit check error: %w", err)})
	}

	// TODO: Consider splitting states and persist proof for faster recovery

	params := &miner.ProveCommitSectorParams{
		SectorNumber: sector.SectorNumber,
		Proof:        proof,
	}

	enc := new(bytes.Buffer)
	if err := params.MarshalCBOR(enc); err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("could not serialize commit sector parameters: %w", err)})
	}

	waddr, err := m.api.StateMinerWorkerAddress(ctx.Context(), m.maddr, tok)
	if err != nil {
		log.Errorf("handleCommitting: api error, not proceeding: %+v", err)
		return nil
	}

	collateral, err := m.api.StateMinerInitialPledgeCollateral(ctx.Context(), m.maddr, sector.SectorNumber, tok)
	if err != nil {
		return xerrors.Errorf("getting initial pledge collateral: %w", err)
	}

	// TODO: check seed / ticket are up to date
	mcid, err := m.api.SendMsg(ctx.Context(), waddr, m.maddr, builtin.MethodsMiner.ProveCommitSector, collateral, big.NewInt(1), 1000000, enc.Bytes())
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("pushing message to mpool: %w", err)})
	}

	dsfsmlog.L.Debug("handleCommitting", zap.String("use time", time.Now().Sub(startAt).String()))

	return ctx.Send(SectorCommitted{
		Proof:   proof,
		Message: mcid,
	})
}

func (m *Sealing) handleCommitWait(ctx statemachine.Context, sector SectorInfo) error {
	startAt := time.Now()
	dsfsmlog.L.Debug("handleCommitWait")

	if sector.CommitMessage == nil {
		log.Errorf("sector %d entered commit wait state without a message cid", sector.SectorNumber)
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("entered commit wait with no commit cid")})
	}

	mw, err := m.api.StateWaitMsg(ctx.Context(), *sector.CommitMessage)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("failed to wait for porep inclusion: %w", err)})
	}

	if mw.Receipt.ExitCode != 0 {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("submitting sector proof failed (exit=%d, msg=%s) (t:%x; s:%x(%d); p:%x)", mw.Receipt.ExitCode, sector.CommitMessage, sector.TicketValue, sector.SeedValue, sector.SeedEpoch, sector.Proof)})
	}

	_, err = m.api.StateSectorGetInfo(ctx.Context(), m.maddr, sector.SectorNumber, mw.TipSetTok)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("proof validation failed, sector not found in sector set after cron: %w", err)})
	}

	dsfsmlog.L.Debug("handleCommitWait", zap.String("use time", time.Now().Sub(startAt).String()))
	return ctx.Send(SectorProving{})
}

func (m *Sealing) handleFinalizeSector(ctx statemachine.Context, sector SectorInfo) error {
	// TODO: Maybe wait for some finality
	startAt := time.Now()
	dsfsmlog.L.Debug("handleFinalizeSector")

	if err := m.sealer.FinalizeSector(sector.sealingCtx(ctx.Context()), m.minerSector(sector.SectorNumber), nil); err != nil {
		return ctx.Send(SectorFinalizeFailed{xerrors.Errorf("finalize sector: %w", err)})
	}

	dsfsmlog.L.Debug("handleFinalizeSector", zap.String("use time", time.Now().Sub(startAt).String()))

	return ctx.Send(SectorFinalized{})
}
