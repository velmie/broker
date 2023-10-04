package broker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	. "github.com/velmie/broker"
	mock_broker "github.com/velmie/broker/mock"
)

func TestSubscriptionCoordinator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := mock_broker.NewMockLogger(ctrl)
	subscription := mock_broker.NewMockSubscription(ctrl)

	ctx := context.Background()

	t.Run("TestSubscribeAll", func(t *testing.T) {
		sc := NewSubscriptionCoordinator()
		sc.SetLogger(logger)
		sf := func(ctx context.Context) (Subscription, error) {
			return subscription, nil
		}
		err := sc.AddSubscription("test_topic", sf)
		require.NoError(t, err)

		logger.EXPECT().Info(gomock.Eq("subscribing to test_topic")).Times(1)
		err = sc.SubscribeAll(ctx)
		require.NoError(t, err)
	})

	t.Run("TestSubscribeAllWithError", func(t *testing.T) {
		sc := NewSubscriptionCoordinator()
		sc.SetLogger(logger)
		sf := func(ctx context.Context) (Subscription, error) {
			return nil, errors.New("subscription error")
		}
		err := sc.AddSubscription("test_topic", sf)
		require.NoError(t, err)

		logger.EXPECT().Info(gomock.Eq("subscribing to test_topic")).Times(1)
		logger.EXPECT().Error(gomock.Eq("failed to subscribe to 'test_topic': subscription error")).Times(1)
		err = sc.SubscribeAll(ctx)
		require.Error(t, err)
	})

	t.Run("TestUnsubscribeAll", func(t *testing.T) {
		sc := NewSubscriptionCoordinator()
		sc.SetLogger(logger)
		sf := func(ctx context.Context) (Subscription, error) {
			return subscription, nil
		}
		err := sc.AddSubscription("test_topic", sf)
		require.NoError(t, err)

		subscription.EXPECT().Topic().Return("test_topic").Times(1)
		subscription.EXPECT().Unsubscribe().Return(nil).Times(1)
		logger.EXPECT().Info(gomock.Eq("subscribing to test_topic")).Times(1)
		logger.EXPECT().Info(gomock.Eq("unsubscribed from test_topic")).Times(1)

		err = sc.SubscribeAll(ctx)
		require.NoError(t, err)

		sc.UnsubscribeAll()
	})

	t.Run("TestAddSubscriptionDuplicate", func(t *testing.T) {
		sc := NewSubscriptionCoordinator()
		sf := func(ctx context.Context) (Subscription, error) {
			return subscription, nil
		}
		err := sc.AddSubscription("test_topic", sf)
		require.NoError(t, err)
		err = sc.AddSubscription("test_topic", sf)
		require.Error(t, err)
		require.Equal(t, "subscription with name \"test_topic\" is already added", err.Error())
	})

	t.Run("SubscribeAllContextCancelled", func(t *testing.T) {
		cctx, cancel := context.WithCancel(context.Background())
		cancel()

		sc := NewSubscriptionCoordinator()
		sf := func(ctx context.Context) (Subscription, error) {
			return subscription, nil
		}
		err := sc.AddSubscription("test_topic", sf)
		require.NoError(t, err)

		err = sc.SubscribeAll(cctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}
