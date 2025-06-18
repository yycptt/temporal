package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/example"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type ChasmTestSuite struct {
	testcore.FunctionalTestBase

	chasmEngine chasm.Engine
}

func TestChasmTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ChasmTestSuite))
}

func (s *ChasmTestSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
	)

	var err error
	s.chasmEngine, err = s.FunctionalTestBase.GetTestCluster().Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(s.chasmEngine)
}

func (s *ChasmTestSuite) TestNewPayloadStore() {
	storeID := "test-store-1"
	err := s.newPayloadStore(storeID)
	s.NoError(err)
}

func (s *ChasmTestSuite) newPayloadStore(
	storeID string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := example.NewPayloadStoreHandler(
		chasm.NewEngineContext(ctx, s.chasmEngine),
		&historyservice.NewPayloadStoreRequest{
			NamespaceId: s.NamespaceID().String(),
			StoreId:     storeID,
		},
	)
	return err
}
