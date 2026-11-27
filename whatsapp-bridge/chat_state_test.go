package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func newChatStateStore(t *testing.T) *MessageStore {
	t.Helper()
	ms := newTestMessageStore(t)
	if err := ensureMessageStoreSchema(ms.db); err != nil {
		t.Fatalf("schema: %v", err)
	}
	return ms
}

type chatStateRow struct {
	archived, mutedUntil, markedUnread sql.NullInt64
	lastReadTime                       sql.NullTime
	name                               sql.NullString
}

func readChatState(t *testing.T, ms *MessageStore, jid string) chatStateRow {
	t.Helper()
	var r chatStateRow
	err := ms.db.QueryRow(
		"SELECT archived, muted_until, marked_unread, last_read_time, name FROM chats WHERE jid = ?", jid,
	).Scan(&r.archived, &r.mutedUntil, &r.markedUnread, &r.lastReadTime, &r.name)
	if err != nil {
		t.Fatalf("read chat state: %v", err)
	}
	return r
}

func TestChatFlagsOnlyMoveForward(t *testing.T) {
	ms := newChatStateStore(t)
	jid := "123@s.whatsapp.net"
	t1 := time.Unix(1_700_000_000, 0)
	t2 := t1.Add(time.Minute)

	if err := ms.SetChatArchived(jid, true, t2); err != nil {
		t.Fatal(err)
	}
	// An older mutation delivered late must not unarchive.
	if err := ms.SetChatArchived(jid, false, t1); err != nil {
		t.Fatal(err)
	}
	// Nor may history sync, which has no mutation time.
	if err := ms.SetChatArchived(jid, false, time.Time{}); err != nil {
		t.Fatal(err)
	}
	r := readChatState(t, ms, jid)
	if !r.archived.Valid || r.archived.Int64 != 1 {
		t.Fatalf("archived = %+v, want 1", r.archived)
	}
	// The app-state row must not invent chat metadata.
	if r.name.Valid {
		t.Fatalf("name = %q, want NULL", r.name.String)
	}

	if err := ms.SetChatArchived(jid, false, t2.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if r := readChatState(t, ms, jid); r.archived.Int64 != 0 {
		t.Fatalf("archived = %d after newer unarchive, want 0", r.archived.Int64)
	}
}

func TestHistoryChatStateOnlyWritesPresentFields(t *testing.T) {
	ms := newChatStateStore(t)
	logger := waLog.Noop
	jid := "123@s.whatsapp.net"

	storeHistoryChatState(ms, jid, &waHistorySync.Conversation{Archived: proto.Bool(true)}, logger)
	// A sparse chunk omitting the fields must not clear them.
	storeHistoryChatState(ms, jid, &waHistorySync.Conversation{}, logger)

	r := readChatState(t, ms, jid)
	if r.archived.Int64 != 1 {
		t.Fatalf("archived = %+v, want 1", r.archived)
	}
	if r.markedUnread.Valid {
		t.Fatalf("marked_unread = %+v, want NULL", r.markedUnread)
	}
}

func TestMuteEndMillis(t *testing.T) {
	cases := []struct {
		muted bool
		end   int64
		want  int64
	}{
		{false, 0, 0},
		{false, 1_800_000_000_000, 0},
		{true, 0, 0}, // whatsmeow reads an explicit 0 as the epoch: expired
		{true, -1, -1},
		{true, 1_800_000_000_000, 1_800_000_000_000},
	}
	for _, c := range cases {
		if got := muteEndMillis(c.muted, c.end); got != c.want {
			t.Errorf("muteEndMillis(%v, %d) = %d, want %d", c.muted, c.end, got, c.want)
		}
	}
}

func TestHandleChatStateEvents(t *testing.T) {
	ms := newChatStateStore(t)
	logger := waLog.Noop
	jid := types.NewJID("123", types.DefaultUserServer)
	at := time.Unix(1_700_000_000, 0)

	stateEvents := []interface{}{
		&events.Archive{JID: jid, Timestamp: at, Action: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}},
		&events.Mute{JID: jid, Timestamp: at, Action: &waSyncAction.MuteAction{Muted: proto.Bool(true), MuteEndTimestamp: proto.Int64(-1)}},
		&events.MarkChatAsRead{JID: jid, Timestamp: at, Action: &waSyncAction.MarkChatAsReadAction{Read: proto.Bool(false)}},
	}
	for _, evt := range stateEvents {
		if !handleChatStateEvent(nil, ms, evt, logger) {
			t.Fatalf("%T not handled", evt)
		}
	}
	r := readChatState(t, ms, jid.String())
	if r.archived.Int64 != 1 || r.mutedUntil.Int64 != -1 || r.markedUnread.Int64 != 1 {
		t.Fatalf("state = %+v", r)
	}
	if r.lastReadTime.Valid {
		t.Fatalf("mark-as-unread advanced the read marker to %s", r.lastReadTime.Time)
	}

	// Marking read clears the flag and reads the chat up to the range's last message.
	read := &events.MarkChatAsRead{JID: jid, Timestamp: at.Add(time.Hour), Action: &waSyncAction.MarkChatAsReadAction{
		Read:         proto.Bool(true),
		MessageRange: &waSyncAction.SyncActionMessageRange{LastMessageTimestamp: proto.Int64(at.Add(time.Minute).Unix())},
	}}
	handleChatStateEvent(nil, ms, read, logger)
	r = readChatState(t, ms, jid.String())
	if r.markedUnread.Int64 != 0 {
		t.Fatalf("marked_unread = %d, want 0", r.markedUnread.Int64)
	}
	if !r.lastReadTime.Valid || !r.lastReadTime.Time.Equal(at.Add(time.Minute)) {
		t.Fatalf("last_read_time = %+v, want %s", r.lastReadTime, at.Add(time.Minute))
	}

	if handleChatStateEvent(nil, ms, &events.Connected{}, logger) {
		t.Fatal("unrelated event reported as handled")
	}
}

func TestBackfillReadMarkerFromUnreadCount(t *testing.T) {
	ms := newChatStateStore(t)
	jid := "123@s.whatsapp.net"
	base := time.Unix(1_700_000_000, 0).UTC()
	if _, err := ms.db.Exec("INSERT INTO chats (jid, name) VALUES (?, 'x')", jid); err != nil {
		t.Fatal(err)
	}
	// Inbound at +1..+5 min, a reaction at +6 and an outbound at +7.
	for i := 1; i <= 5; i++ {
		if err := ms.StoreMessage(fmt.Sprintf("in%d", i), jid, jid, "hi", base.Add(time.Duration(i)*time.Minute), false, "", "", "", nil, nil, nil, 0, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.StoreMessage("react", jid, jid, "", base.Add(6*time.Minute), false, "reaction", "in1", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := ms.StoreMessage("out", jid, "me", "yo", base.Add(7*time.Minute), true, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatal(err)
	}

	// Phone says 2 unread: the marker lands on the 3rd-newest inbound (+3).
	if err := ms.BackfillReadMarkerFromUnreadCount(jid, 2); err != nil {
		t.Fatal(err)
	}
	r := readChatState(t, ms, jid)
	if !r.lastReadTime.Valid || !r.lastReadTime.Time.Equal(base.Add(3*time.Minute)) {
		t.Fatalf("last_read_time = %+v, want %s", r.lastReadTime, base.Add(3*time.Minute))
	}

	// An existing marker is never moved.
	if err := ms.BackfillReadMarkerFromUnreadCount(jid, 4); err != nil {
		t.Fatal(err)
	}
	if r := readChatState(t, ms, jid); !r.lastReadTime.Time.Equal(base.Add(3 * time.Minute)) {
		t.Fatalf("existing marker moved to %s", r.lastReadTime.Time)
	}

	// More unread than stored: nothing to anchor on, marker stays NULL.
	other := "456@s.whatsapp.net"
	if _, err := ms.db.Exec("INSERT INTO chats (jid, name) VALUES (?, 'y')", other); err != nil {
		t.Fatal(err)
	}
	if err := ms.StoreMessage("o1", other, other, "hi", base, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := ms.BackfillReadMarkerFromUnreadCount(other, 3); err != nil {
		t.Fatal(err)
	}
	if r := readChatState(t, ms, other); r.lastReadTime.Valid {
		t.Fatalf("marker set to %s with too few messages", r.lastReadTime.Time)
	}
}

// A full sync replays every chat's latest mark-as-read, however old. It must
// not set a read marker, or the pairing-time UnreadCount anchor is skipped and
// every inbound message since that old mark-as-read counts as unread.
func TestFullSyncMarkAsReadKeepsReadMarker(t *testing.T) {
	ms := newChatStateStore(t)
	jid := types.NewJID("123", types.DefaultUserServer)
	old := time.Unix(1_670_000_000, 0).UTC()
	action := &waSyncAction.MarkChatAsReadAction{
		Read:         proto.Bool(true),
		MessageRange: &waSyncAction.SyncActionMessageRange{LastMessageTimestamp: proto.Int64(old.Unix())},
	}

	handleChatStateEvent(nil, ms, &events.MarkChatAsRead{JID: jid, Timestamp: old, Action: action, FromFullSync: true}, waLog.Noop)
	r := readChatState(t, ms, jid.String())
	if r.lastReadTime.Valid {
		t.Fatalf("full-sync replay set last_read_time = %s", r.lastReadTime.Time)
	}
	if !r.markedUnread.Valid || r.markedUnread.Int64 != 0 {
		t.Fatalf("marked_unread = %+v, want 0 (the replay still carries the flag)", r.markedUnread)
	}

	// The history-sync anchor can now place the marker.
	if err := ms.BackfillReadMarkerAt(jid.String(), old.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	// A live mark-as-read still moves it forward.
	live := old.Add(48 * time.Hour)
	action.MessageRange.LastMessageTimestamp = proto.Int64(live.Unix())
	handleChatStateEvent(nil, ms, &events.MarkChatAsRead{JID: jid, Timestamp: live, Action: action}, waLog.Noop)
	if r := readChatState(t, ms, jid.String()); !r.lastReadTime.Time.Equal(live) {
		t.Fatalf("last_read_time = %s after live mark-as-read, want %s", r.lastReadTime.Time, live)
	}
}

func historyMsg(fromMe bool, ts int64, m *waProto.Message) *waProto.HistorySyncMsg {
	return &waProto.HistorySyncMsg{Message: &waProto.WebMessageInfo{
		Key:              &waProto.MessageKey{FromMe: proto.Bool(fromMe)},
		MessageTimestamp: proto.Uint64(uint64(ts)),
		Message:          m,
	}}
}

func TestHistoryReadAnchor(t *testing.T) {
	text := &waProto.Message{Conversation: proto.String("hi")}
	poll := &waProto.Message{PollCreationMessage: &waProto.PollCreationMessage{Name: proto.String("lunch?")}}
	reaction := &waProto.Message{ReactionMessage: &waProto.ReactionMessage{Text: proto.String("👍")}}
	edit := &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{}}
	// Newest first, as history sync sends them.
	messages := []*waProto.HistorySyncMsg{
		historyMsg(false, 160, reaction),
		historyMsg(false, 150, edit),
		historyMsg(false, 140, poll), // unread, not stored by the bridge
		historyMsg(true, 130, text),
		historyMsg(false, 120, text), // unread
		historyMsg(false, 110, text), // newest read inbound: the anchor
		historyMsg(false, 100, text),
		nil,
	}
	got, ok := historyReadAnchor(messages, 2)
	if !ok || got.Unix() != 110 {
		t.Fatalf("anchor = %d, %v; want 110 (the poll counts, the reaction and edit don't)", got.Unix(), ok)
	}
	if _, ok := historyReadAnchor(messages, 4); ok {
		t.Fatal("anchored with only 4 inbound messages for 4 unread")
	}
}

func TestBackfillReadMarkerAtOnlyFillsEmptyMarker(t *testing.T) {
	ms := newChatStateStore(t)
	jid := "123@s.whatsapp.net"
	t1 := time.Unix(1_700_000_000, 0).UTC()
	if _, err := ms.db.Exec("INSERT INTO chats (jid, name) VALUES (?, 'x')", jid); err != nil {
		t.Fatal(err)
	}
	if err := ms.BackfillReadMarkerAt(jid, t1); err != nil {
		t.Fatal(err)
	}
	if err := ms.BackfillReadMarkerAt(jid, t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if r := readChatState(t, ms, jid); !r.lastReadTime.Time.Equal(t1) {
		t.Fatalf("last_read_time = %s, want %s", r.lastReadTime.Time, t1)
	}
}

// A mark-read through the bridge sends no app-state mutation, so the
// phone's flag must win again at the next full-sync replay.
func TestLocalMarkedUnreadClearYieldsToPhone(t *testing.T) {
	ms := newChatStateStore(t)
	jid := "123@s.whatsapp.net"
	at := time.Unix(1_700_000_000, 0)
	if err := ms.SetChatMarkedUnread(jid, true, at); err != nil {
		t.Fatal(err)
	}
	if err := ms.ClearChatMarkedUnreadLocally(jid); err != nil {
		t.Fatal(err)
	}
	if r := readChatState(t, ms, jid); r.markedUnread.Int64 != 0 {
		t.Fatalf("marked_unread = %d after local clear, want 0", r.markedUnread.Int64)
	}
	if err := ms.SetChatMarkedUnread(jid, true, at); err != nil {
		t.Fatal(err)
	}
	if r := readChatState(t, ms, jid); r.markedUnread.Int64 != 1 {
		t.Fatalf("marked_unread = %d after the phone's mutation replayed, want 1", r.markedUnread.Int64)
	}
}

func TestAppStateChatJIDFlagsUnmappedLID(t *testing.T) {
	t.Cleanup(func() { chatStateUnresolvedLID.Store(false) })
	chatStateUnresolvedLID.Store(false)
	phone := types.NewJID("123", types.DefaultUserServer)
	if got := appStateChatJID(nil, phone); got != phone.String() || chatStateUnresolvedLID.Load() {
		t.Fatalf("phone JID: got %s, flagged=%v", got, chatStateUnresolvedLID.Load())
	}
	lid := types.NewJID("999", types.HiddenUserServer)
	if got := appStateChatJID(nil, lid); got != lid.String() || !chatStateUnresolvedLID.Load() {
		t.Fatalf("unmapped LID: got %s, flagged=%v", got, chatStateUnresolvedLID.Load())
	}
}

func TestLIDChatMigrationCarriesChatFlags(t *testing.T) {
	ms := newChatStateStore(t)
	waPath := filepath.Join(t.TempDir(), "whatsapp.db")
	waDB, err := sql.Open("sqlite3", waPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = waDB.Close() }()
	if _, err := waDB.Exec(`CREATE TABLE whatsmeow_lid_map (lid TEXT PRIMARY KEY, pn TEXT NOT NULL);
		INSERT INTO whatsmeow_lid_map VALUES ('111', '222');`); err != nil {
		t.Fatal(err)
	}
	lid, phone := "111@lid", "222@s.whatsapp.net"
	t1 := time.Unix(1_700_000_000, 0)
	// Archived on the @lid row (newer), muted on the phone row (newer there).
	if err := ms.SetChatArchived(lid, true, t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := ms.SetChatArchived(phone, false, t1); err != nil {
		t.Fatal(err)
	}
	if err := ms.SetChatMutedUntil(lid, 0, t1); err != nil {
		t.Fatal(err)
	}
	if err := ms.SetChatMutedUntil(phone, -1, t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := ms.MigrateLegacyLIDChatsToPhoneJIDs(waPath, waLog.Noop); err != nil {
		t.Fatal(err)
	}
	r := readChatState(t, ms, phone)
	if r.archived.Int64 != 1 {
		t.Fatalf("archived = %+v, want 1 from the newer @lid mutation", r.archived)
	}
	if r.mutedUntil.Int64 != -1 {
		t.Fatalf("muted_until = %+v, want -1 kept from the newer phone mutation", r.mutedUntil)
	}
	if r.markedUnread.Valid {
		t.Fatalf("marked_unread = %+v, want NULL: neither row set it", r.markedUnread)
	}
}
