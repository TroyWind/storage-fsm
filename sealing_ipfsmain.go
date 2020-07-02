package sealing

import (
	"context"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/storage-fsm/lib/dlog/dsfsmlog"
	"go.uber.org/zap"
	"io"

	"golang.org/x/xerrors"

	sectorstorage "github.com/filecoin-project/sector-storage"
	"github.com/filecoin-project/sector-storage/ffiwrapper"
	"github.com/filecoin-project/specs-actors/actors/abi"
)

func (m *Sealing) genNewSector() (*SectorStart, error) {
	sid, err := m.sc.Next()
	if err != nil {
		return nil, xerrors.Errorf("getting sector number: %w", err)
	}
	rt, err := ffiwrapper.SealProofTypeFromSectorSize(m.sealer.SectorSize())
	if err != nil {
		return nil, err
	}
	// todo 0702版本的 lotus 官方还未实现 NewSector，如果以后实现了需要检查下这部分代码是否正确
	err = m.sealer.NewSector(context.TODO(), m.minerSector(sid))
	if err != nil {
		return nil, xerrors.Errorf("initializing sector: %w", err)
	}
	return &SectorStart{
		ID:         sid,
		SectorType: rt,
	}, nil
}

// 外边应该是 AllocatePieceIfNeeded 和 SealPieceIfNeeded 一起用的，因此需要加锁。不过通过回调可以把锁放这里，应该更合理
// 必须要在外层加锁
func (m *Sealing) AllocatePieceAndSendIfNeeded(size abi.UnpaddedPieceSize) (sectorID abi.SectorNumber, offset uint64, err error) {
	if (padreader.PaddedSize(uint64(size))) != size {
		return 0, 0, xerrors.Errorf("cannot allocate unpadded piece")
	}

	if m.curSector == nil {
		newS, err := m.genNewSector()
		if err != nil {
			return 0, 0, err
		}
		m.curSector = newS
	}

	// TODO 如果密封过程中出错，curSector 是否会有问题
	// 根据 validSectorSize 中看 offset 应该取 Unpadded
	for _, p := range m.curSector.Pieces {
		offset += uint64(p.Piece.Size.Unpadded())
	}

	// 检查空间是否够，如果不够则发送当前的sector，并新建一个 sector，offset设置为0
	if !m.validSectorSize(m.curUnpaddedPieceSizes(), size) {
		dsfsmlog.L.Debug("call seal sector", zap.Uint64("sid", uint64(m.curSector.ID)), zap.Int("piece len", len(m.curSector.Pieces)))
		// 发送当前的 sector
		if err = m.sectors.Send(uint64(m.curSector.ID), *m.curSector); err != nil {
			return 0, 0, err
		}

		offset = 0
		// 新建一个当前的 sector
		newS, err := m.genNewSector()
		if err != nil {
			return 0, 0, err
		}
		m.curSector = newS
	}

	sid := m.curSector.ID
	dsfsmlog.L.Debug("AllocatePiece", zap.Uint64("size", uint64(size)), zap.Uint64("sector id", uint64(sid)), zap.Uint64("offset", offset))

	// offset hard-coded to 0 since we only put one thing in a sector for now
	return sid, offset, nil
}

// 进来piece就将其加到当前的sector中，如果加不了则新建一个sector，并启动当前sector的密封
func (m *Sealing) AddPiece(ctx context.Context, size abi.UnpaddedPieceSize, r io.Reader, sectorID abi.SectorNumber, d DealInfo) error {
	log.Infof("add piece %d", d.DealID)

	ppi, err := m.sealer.AddPiece(sectorstorage.WithPriority(ctx, DealSectorPriority), m.minerSector(sectorID), m.curUnpaddedPieceSizes(), size, r)
	if err != nil {
		return xerrors.Errorf("adding piece to sector: %w", err)
	}

	dsfsmlog.L.Debug("AddPiece to queue", zap.Uint64("sector id", uint64(sectorID)), zap.Uint64("deal id", uint64(d.DealID)))
	// 放入待处理列表
	m.curSector.Pieces = append(m.curSector.Pieces, Piece{
		Piece:    ppi,
		DealInfo: &d,
	})

	return nil
}

func (m *Sealing) validSectorSize(existingPieceSizes []abi.UnpaddedPieceSize, pieceSize abi.UnpaddedPieceSize) bool {
	// 从 sealer_cgo 中的 AddPiece 拷过来的
	var offset abi.UnpaddedPieceSize
	for _, size := range existingPieceSizes {
		offset += size
	}

	maxPieceSize := abi.PaddedPieceSize(m.sealer.SectorSize())

	if offset.Padded() + pieceSize.Padded() > maxPieceSize {
		//return abi.PieceInfo{}, xerrors.Errorf("can't add %d byte piece to sector %v with %d bytes of existing pieces", pieceSize, sector, offset)
		return false
	}
	return true
}

func (m *Sealing) curUnpaddedPieceSizes() (result []abi.UnpaddedPieceSize) {
	for _, s := range m.curSector.Pieces {
		result = append(result, s.Piece.Size.Unpadded())
	}
	return
}
