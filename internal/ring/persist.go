package ring

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// TokensFilePath returns the canonical file path for the tokens file for nodeID
// inside the provided dataDir (the same data dir you pass to storage.Open).
// Example: dataDir/tokens/nodeA-tokens.json
func TokensFilePath(dataDir string, nodeID NodeID) string {
	dir := "tokens"
	os.Mkdir(dir, 0755)
	return fmt.Sprintf("%s/%s-tokens.json",dir, nodeID)
}

// PersistTokens writes tokens to disk (atomic write).
// dataDir should be the same directory you use for the node's persistent storage.
// tokens is a slice of uint64 (token values).
func PersistTokens(dataDir string, nodeID NodeID, tokens []uint64) error {
	if dataDir == "" {
		return fmt.Errorf("dataDir empty")
	}
	dir := filepath.Dir(TokensFilePath(dataDir, nodeID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir tokens dir: %w", err)
	}

	filePath := TokensFilePath(dataDir, nodeID)
	tmpPath := filePath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open tmp tokens file: %w", err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(tokens); err != nil {
		f.Close()
		return fmt.Errorf("encode tokens: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close tmp tokens file: %w", err)
	}
	// atomic rename
	if err := os.Rename(tmpPath, filePath); err != nil {
		return fmt.Errorf("rename tokens file: %w", err)
	}
	return nil
}

// LoadTokens loads tokens for nodeID from disk.
// If file doesn't exist, returns (nil, os.ErrNotExist).
func LoadTokens(dataDir string, nodeID NodeID) ([]uint64, error) {
	filePath := TokensFilePath(dataDir, nodeID)
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var tokens []uint64
	dec := json.NewDecoder(f)
	if err := dec.Decode(&tokens); err != nil {
		return nil, fmt.Errorf("decode tokens: %w", err)
	}
	return tokens, nil
}