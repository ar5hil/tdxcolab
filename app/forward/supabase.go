package forward

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gotd/td/telegram/peers"

	"github.com/iyear/tdl/app/internal/tctx"
	"github.com/iyear/tdl/core/storage"
	"github.com/iyear/tdl/core/util/tutil"
	"github.com/iyear/tdl/pkg/supabase"
	"github.com/iyear/tdl/pkg/tmessage"
)

func collectDialogsFromSupabase(
	ctx context.Context,
	keys []string,
	autoClean bool,
	configPath string,
) ([]*tmessage.Dialog, map[cleanupKey]cleanupFunc, error) {
	client, err := supabase.Load(configPath)
	if err != nil {
		return nil, nil, err
	}

	manager := peers.Options{Storage: storage.NewPeers(tctx.KV(ctx))}.Build(tctx.Pool(ctx).Default(ctx))

	dialogs := make([]*tmessage.Dialog, 0, len(keys))
	cleanups := make(map[cleanupKey]cleanupFunc)

	for _, key := range keys {
		chatID, topicID, err := parseSupabaseKey(key)
		if err != nil {
			return nil, nil, err
		}

		messageIDs, err := client.FetchMessageIDs(ctx, chatID, topicID)
		if err != nil {
			return nil, nil, fmt.Errorf("fetch supabase key %q: %w", key, err)
		}

		peer, err := tutil.GetInputPeer(ctx, manager, strconv.FormatInt(chatID, 10))
		if err != nil {
			return nil, nil, fmt.Errorf("resolve peer %d from supabase key %q: %w", chatID, key, err)
		}

		dialogs = append(dialogs, &tmessage.Dialog{
			Peer:     peer.InputPeer(),
			Messages: messageIDs,
		})

		if !autoClean {
			continue
		}

		fromID := peer.ID()
		for _, id := range messageIDs {
			msgID := id
			sourceChatID := chatID
			sourceTopicID := topicID

			cleanups[cleanupKey{
				from: fromID,
				msg:  msgID,
			}] = func(ctx context.Context) error {
				return client.DeleteMessage(ctx, sourceChatID, sourceTopicID, msgID)
			}
		}
	}

	if !autoClean || len(cleanups) == 0 {
		cleanups = nil
	}

	return dialogs, cleanups, nil
}

func parseSupabaseKey(input string) (int64, int, error) {
	key := strings.TrimSpace(input)
	if key == "" {
		return 0, 0, fmt.Errorf("empty supabase source key")
	}

	parts := strings.Split(key, ".")
	switch len(parts) {
	case 1:
		chatID, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid supabase source key %q: %w", input, err)
		}
		return chatID, 0, nil
	case 2:
		chatID, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid supabase source key %q chat id: %w", input, err)
		}

		topicID, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid supabase source key %q topic id: %w", input, err)
		}

		return chatID, topicID, nil
	default:
		return 0, 0, fmt.Errorf("invalid supabase source key %q: expected CHAT_ID.TOPIC_ID", input)
	}
}
