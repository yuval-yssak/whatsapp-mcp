package main

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Chat app state: archive, mute and mark-as-unread, as set on the phone.
//
// WhatsApp syncs these between devices as app-state mutations, not messages.
// Each flag is stored with the unix-millisecond time of the mutation that set
// it (`*_ts`), and a write only lands if it is at least as new as the stored
// one, so out-of-order patches and stale history-sync data can't roll a flag
// back. History sync carries no mutation time and writes with ts 0, so it
// only fills in chats app state never reported on.

// chatFlag names a value column and its timestamp column. Only these
// constants are ever interpolated into SQL.
type chatFlag struct{ value, ts string }

var (
	chatFlagArchived     = chatFlag{"archived", "archived_ts"}
	chatFlagMutedUntil   = chatFlag{"muted_until", "muted_ts"}
	chatFlagMarkedUnread = chatFlag{"marked_unread", "marked_unread_ts"}
)

// ensureChatStateColumns adds the app-state columns and the unread-count
// index; part of
// ensureMessageStoreSchema.
func ensureChatStateColumns(db *sql.DB) error {
	// Serves the MCP server's per-chat unread count: the latest outbound
	// message and the inbound messages after a point, both per chat.
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_messages_chat_from_me_ts
		ON messages(chat_jid, is_from_me, timestamp)`); err != nil {
		return fmt.Errorf("failed to create idx_messages_chat_from_me_ts: %w", err)
	}
	for _, flag := range []chatFlag{chatFlagArchived, chatFlagMutedUntil, chatFlagMarkedUnread} {
		if err := ensureColumn(db, "chats", flag.value, "INTEGER"); err != nil {
			return fmt.Errorf("failed to ensure chats.%s column: %w", flag.value, err)
		}
		if err := ensureColumn(db, "chats", flag.ts, "INTEGER NOT NULL DEFAULT 0"); err != nil {
			return fmt.Errorf("failed to ensure chats.%s column: %w", flag.ts, err)
		}
	}
	return nil
}

// setChatFlag upserts one flag. Like MarkChatRead it inserts only its own
// columns, so an app-state event for a chat with no messages yet doesn't
// fabricate name/last_message_time.
func (store *MessageStore) setChatFlag(jid string, flag chatFlag, value int64, atMillis int64) error {
	_, err := store.db.Exec(fmt.Sprintf(
		`INSERT INTO chats (jid, %[1]s, %[2]s)
		VALUES (?, ?, ?)
		ON CONFLICT(jid) DO UPDATE SET
			%[1]s = excluded.%[1]s,
			%[2]s = excluded.%[2]s
		WHERE excluded.%[2]s >= chats.%[2]s`, flag.value, flag.ts),
		jid, value, atMillis,
	)
	return err
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func millis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// SetChatArchived records the chat's archive state.
func (store *MessageStore) SetChatArchived(jid string, archived bool, at time.Time) error {
	return store.setChatFlag(jid, chatFlagArchived, boolInt(archived), millis(at))
}

// SetChatMarkedUnread records whether the chat is manually marked unread.
func (store *MessageStore) SetChatMarkedUnread(jid string, marked bool, at time.Time) error {
	return store.setChatFlag(jid, chatFlagMarkedUnread, boolInt(marked), millis(at))
}

// SetChatMutedUntil records the mute end in unix milliseconds, as WhatsApp
// reports it: -1 = muted indefinitely, 0 = not muted.
func (store *MessageStore) SetChatMutedUntil(jid string, mutedUntilMillis int64, at time.Time) error {
	return store.setChatFlag(jid, chatFlagMutedUntil, mutedUntilMillis, millis(at))
}

// muteEndMillis maps a MuteAction to the stored muted_until value, the way
// whatsmeow's own store reads it: a negative end is "forever", any other end
// is an instant (0 = the epoch, i.e. already expired).
func muteEndMillis(muted bool, endTimestamp int64) int64 {
	if !muted {
		return 0
	}
	if endTimestamp < 0 {
		return -1
	}
	return endTimestamp
}

// BackfillReadMarkerFromUnreadCount places the read marker of a chat that has
// none so that exactly its newest unreadCount inbound messages are unread,
// as history sync reports. Without it a chat that was unread at pairing has
// no marker, and the MCP server's count falls back to every inbound message
// stored. A chat that already has a marker is left alone, and with fewer than
// unreadCount+1 inbound messages stored there is nothing to anchor on.
// Counted like the MCP server: reactions and deleted messages are skipped.
// BackfillReadMarkerAt sets the read marker of a chat that has none.
func (store *MessageStore) BackfillReadMarkerAt(chatJID string, readAt time.Time) error {
	_, err := store.db.Exec(
		`UPDATE chats SET last_read_time = ? WHERE jid = ? AND last_read_time IS NULL`,
		readAt, chatJID,
	)
	return err
}

// ClearChatMarkedUnreadLocally clears marked_unread without touching its
// mutation time. Reading a chat through the bridge sends receipts but no
// app-state mutation, so the phone may keep its flag. Keeping the phone's
// time lets the next full-sync replay of that mutation restore the flag,
// and any newer phone mutation wins as usual.
func (store *MessageStore) ClearChatMarkedUnreadLocally(chatJID string) error {
	_, err := store.db.Exec(`UPDATE chats SET marked_unread = 0 WHERE jid = ?`, chatJID)
	return err
}

// historyReadAnchor finds where the read marker goes for a history-sync
// conversation the phone reports unreadCount unread messages for: the time
// of the newest inbound message that isn't unread. It counts the
// conversation's own messages, including the polls, locations and other
// kinds the bridge doesn't store, which the phone's count also includes.
// Counting only stored rows would put the marker too early and count
// already-read messages as unread. Reactions and protocol messages (edits,
// revokes) aren't in the phone's count and are skipped here too. It reports
// false when the chunk holds too few messages to anchor on.
//
// The marker has one-second resolution, as stored timestamps do, so an
// unread message sent in the same second as the anchor counts as read.
func historyReadAnchor(messages []*waProto.HistorySyncMsg, unreadCount uint32) (time.Time, bool) {
	var inbound []time.Time
	for _, msg := range messages {
		info := msg.GetMessage()
		if info == nil || info.GetKey().GetFromMe() || info.GetMessageTimestamp() == 0 {
			continue
		}
		m := info.GetMessage()
		if m == nil || m.GetReactionMessage() != nil || m.GetProtocolMessage() != nil ||
			m.GetEncReactionMessage() != nil {
			continue
		}
		inbound = append(inbound, time.Unix(int64(info.GetMessageTimestamp()), 0))
	}
	if uint32(len(inbound)) <= unreadCount {
		return time.Time{}, false
	}
	sort.Slice(inbound, func(i, j int) bool { return inbound[i].After(inbound[j]) })
	return inbound[unreadCount], true
}

func (store *MessageStore) BackfillReadMarkerFromUnreadCount(chatJID string, unreadCount uint32) error {
	_, err := store.db.Exec(
		`UPDATE chats SET last_read_time = (
			SELECT m.timestamp FROM messages m
			WHERE m.chat_jid = chats.jid AND m.is_from_me = 0
			  AND (m.media_type IS NULL OR m.media_type != 'reaction')
			  AND m.deleted_at IS NULL
			ORDER BY m.timestamp DESC
			LIMIT 1 OFFSET ?
		)
		WHERE jid = ? AND last_read_time IS NULL AND EXISTS (
			SELECT 1 FROM messages m
			WHERE m.chat_jid = chats.jid AND m.is_from_me = 0
			  AND (m.media_type IS NULL OR m.media_type != 'reaction')
			  AND m.deleted_at IS NULL
			ORDER BY m.timestamp DESC
			LIMIT 1 OFFSET ?
		)`,
		unreadCount, chatJID, unreadCount,
	)
	return err
}

// appStateChatJID maps an app-state target to the chat JID messages are
// stored under. App-state patches carry no alt JIDs, so LID chats resolve
// through the whatsmeow LID store only. It doesn't log: the startup sync
// calls it for every chat.
//
// A LID with no mapping yet is stored under the @lid JID and flags
// chatStateUnresolvedLID. At pairing that is the normal case, because the
// app-state sync runs before history sync fills in the LID map.
// resyncChatStateAfterLIDMapping then repeats the sync, and the startup LID
// migration carries whatever is left onto the phone-JID row.
func appStateChatJID(client *whatsmeow.Client, jid types.JID) string {
	if jid.Server != types.HiddenUserServer {
		return jid.String()
	}
	if client != nil && client.Store != nil && client.Store.LIDs != nil {
		if pn, err := client.Store.LIDs.GetPNForLID(context.Background(), jid); err == nil && !pn.IsEmpty() {
			return pn.ToNonAD().String()
		}
	}
	chatStateUnresolvedLID.Store(true)
	return jid.String()
}

// handleChatStateEvent stores archive / mute / mark-as-unread app-state
// events. It reports whether evt was one of them.
func handleChatStateEvent(client *whatsmeow.Client, store *MessageStore, evt interface{}, logger waLog.Logger) bool {
	switch v := evt.(type) {
	case *events.Archive:
		jid := appStateChatJID(client, v.JID)
		if err := store.SetChatArchived(jid, v.Action.GetArchived(), v.Timestamp); err != nil {
			logger.Warnf("Failed to store archive state for %s: %v", jid, err)
		}
	case *events.Mute:
		jid := appStateChatJID(client, v.JID)
		until := muteEndMillis(v.Action.GetMuted(), v.Action.GetMuteEndTimestamp())
		if err := store.SetChatMutedUntil(jid, until, v.Timestamp); err != nil {
			logger.Warnf("Failed to store mute state for %s: %v", jid, err)
		}
	case *events.MarkChatAsRead:
		jid := appStateChatJID(client, v.JID)
		read := v.Action.GetRead()
		if err := store.SetChatMarkedUnread(jid, !read, v.Timestamp); err != nil {
			logger.Warnf("Failed to store marked-unread state for %s: %v", jid, err)
		}
		// "Mark as read" on the phone reads the whole chat up to its last
		// message. Only a live mutation moves the read marker. A full sync
		// replays every chat's latest one, months old for most chats, and at
		// pairing it lands before history sync. A replayed marker would then
		// block the history-sync UnreadCount anchor
		// (BackfillReadMarkerFromUnreadCount) and count every inbound
		// message since that old mark-as-read as unread.
		if read && !v.FromFullSync {
			readAt := v.Timestamp
			if ts := v.Action.GetMessageRange().GetLastMessageTimestamp(); ts > 0 {
				readAt = time.Unix(ts, 0)
			}
			if err := store.MarkChatRead(jid, readAt); err != nil {
				logger.Warnf("Failed to mark chat %s read: %v", jid, err)
			}
		}
	default:
		return false
	}
	return true
}

// storeHistoryChatState backfills archive and marked-unread from a history
// sync conversation. Sparse chunks omit the fields (the getters then return
// false), so only fields actually present are written. Mute is left to the
// app-state sync: its history-sync unit isn't documented.
func storeHistoryChatState(store *MessageStore, chatJID string, conversation *waHistorySync.Conversation, logger waLog.Logger) {
	if conversation.Archived != nil {
		if err := store.SetChatArchived(chatJID, conversation.GetArchived(), time.Time{}); err != nil {
			logger.Warnf("Failed to backfill archive state for %s: %v", chatJID, err)
		}
	}
	if conversation.MarkedAsUnread != nil {
		if err := store.SetChatMarkedUnread(chatJID, conversation.GetMarkedAsUnread(), time.Time{}); err != nil {
			logger.Warnf("Failed to backfill marked-unread state for %s: %v", chatJID, err)
		}
	}
}

// chatStatePatches hold the archive / markChatAsRead (regular_low) and mute
// (regular_high) mutations.
var chatStatePatches = []appstate.WAPatchName{appstate.WAPatchRegularLow, appstate.WAPatchRegularHigh}

// chatStateSynced is set once a full chat app-state sync has succeeded in
// this process, so reconnects don't repeat it.
var chatStateSynced atomic.Bool

// chatStateSyncing keeps back-to-back Connected events from running two
// full snapshot downloads at once.
var chatStateSyncing atomic.Bool

// chatStateUnresolvedLID is set when an app-state target was a LID with no
// phone mapping yet. chatStateLIDResynced limits the repeat sync it triggers
// to one per process.
var (
	chatStateUnresolvedLID atomic.Bool
	chatStateLIDResynced   atomic.Bool
)

// resyncChatStateAfterLIDMapping repeats the chat app-state sync once, after
// a history sync, if the first one hit LIDs it couldn't map. History sync
// carries the LID map, so the repeat stores those flags on the phone-JID chat
// rather than on an @lid row that list_chats never shows.
func resyncChatStateAfterLIDMapping(ctx context.Context, client *whatsmeow.Client, logger waLog.Logger) {
	if !chatStateSynced.Load() || !chatStateUnresolvedLID.Load() ||
		!chatStateLIDResynced.CompareAndSwap(false, true) {
		return
	}
	chatStateUnresolvedLID.Store(false)
	chatStateSynced.Store(false)
	logger.Infof("Repeating the chat app-state sync: the first one hit LID chats with no phone mapping yet")
	syncChatStateOnce(ctx, client, logger)
}

// syncChatStateOnce re-downloads the chat app-state snapshots so the bridge
// learns the current archive / mute / marked-unread state of every chat,
// including what was set before this bridge version stored it. Incremental
// patches only carry changes; without this a long-linked device never sees
// state set before the upgrade. The client must have
// EmitAppStateEventsOnFullSync set, or the snapshot emits no events.
// Failures leave the flag unset so the next connect retries.
func syncChatStateOnce(ctx context.Context, client *whatsmeow.Client, logger waLog.Logger) {
	if chatStateSynced.Load() || !chatStateSyncing.CompareAndSwap(false, true) {
		return
	}
	defer chatStateSyncing.Store(false)
	for _, name := range chatStatePatches {
		if err := client.FetchAppState(ctx, name, true, false); err != nil {
			logger.Warnf("Chat app-state sync of %s failed (will retry on next connect): %v", name, err)
			return
		}
	}
	chatStateSynced.Store(true)
	logger.Infof("Chat app-state (archive/mute/marked-unread) synced")
}
