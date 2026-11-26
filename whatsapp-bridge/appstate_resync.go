package main

import (
	"context"
	"errors"
	"fmt"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
)

// appStateClient is the part of *whatsmeow.Client that sendAppStateWithResync
// uses, so tests can fake it.
type appStateClient interface {
	SendAppState(ctx context.Context, patch appstate.PatchInfo) error
	FetchAppState(ctx context.Context, name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) error
}

// appStateDiverged reports whether a send failed because the bridge's stored
// copy of the collection no longer matches the server's: a 409 conflict that
// whatsmeow could not resolve by applying the returned patches, typically
// "failed to verify patch vN: mismatching LTHash". whatsmeow's own retry only
// applies the conflicting patches on top of the stored state, so once that
// state is corrupt every send fails the same way until it is rebuilt.
func appStateDiverged(err error) bool {
	return errors.Is(err, whatsmeow.ErrAppStateUpdate) || errors.Is(err, appstate.ErrMismatchingLTHash)
}

// sendAppStateWithResync sends an app-state patch. If the collection has
// diverged from the server, it drops the stored version and re-downloads the
// collection's snapshot (a full sync, which keeps the device linked), then
// retries the send once.
func sendAppStateWithResync(ctx context.Context, client appStateClient, patch appstate.PatchInfo, logf func(string, ...any)) error {
	err := client.SendAppState(ctx, patch)
	if err == nil || !appStateDiverged(err) {
		return err
	}
	logf("App state %s diverged from the server (%v); doing a full resync and retrying", patch.Type, err)
	if syncErr := client.FetchAppState(ctx, patch.Type, true, false); syncErr != nil {
		return fmt.Errorf("%w (full resync of %s also failed: %w)", err, patch.Type, syncErr)
	}
	if retryErr := client.SendAppState(ctx, patch); retryErr != nil {
		return fmt.Errorf("after a full resync of %s: %w", patch.Type, retryErr)
	}
	logf("App state %s resynced; send succeeded on retry", patch.Type)
	return nil
}
