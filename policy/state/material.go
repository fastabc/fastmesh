package state

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fastabc/fastmesh/policy/material"
	"github.com/fastabc/fastmesh/policy/snapshot"
)

type encryptedMaterials struct {
	Nonce      []byte `json:"nonce"`
	Ciphertext []byte `json:"ciphertext"`
}

func (s *Store) loadOrCreateMaterialKey() ([]byte, error) {
	key, err := s.loadMaterialKey()
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	key = make([]byte, materialKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("state: generate material cache key: %w", err)
	}
	if err := s.writeAtomic(materialKeyFile, key); err != nil {
		return nil, err
	}
	return key, nil
}

func (s *Store) loadMaterialKey() ([]byte, error) {
	key, err := os.ReadFile(filepath.Join(s.dir, materialKeyFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(key) != materialKeySize {
		return nil, fmt.Errorf("state: invalid material cache key length")
	}
	return key, nil
}

func encryptMaterials(key []byte, revision snapshot.Revision, materials []material.ResolvedMaterial) (*encryptedMaterials, error) {
	plain, err := json.Marshal(materials)
	if err != nil {
		return nil, err
	}
	defer zero(plain)
	aead, err := materialAEAD(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ciphertext := aead.Seal(nil, nonce, plain, []byte(revision))
	return &encryptedMaterials{Nonce: nonce, Ciphertext: ciphertext}, nil
}

func decryptMaterials(key []byte, revision snapshot.Revision, encrypted *encryptedMaterials) ([]material.ResolvedMaterial, error) {
	aead, err := materialAEAD(key)
	if err != nil {
		return nil, err
	}
	if len(encrypted.Nonce) != aead.NonceSize() {
		return nil, fmt.Errorf("state: invalid material cache nonce")
	}
	plain, err := aead.Open(nil, encrypted.Nonce, encrypted.Ciphertext, []byte(revision))
	if err != nil {
		return nil, fmt.Errorf("state: decrypt material cache: %w", err)
	}
	defer zero(plain)
	var materials []material.ResolvedMaterial
	if err := json.Unmarshal(plain, &materials); err != nil {
		return nil, err
	}
	return materials, nil
}

func materialAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func zero(data []byte) {
	for i := range data {
		data[i] = 0
	}
}
