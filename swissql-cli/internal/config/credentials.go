package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// pkcs7Pad pads data to a multiple of blockSize using PKCS7 padding.
// For AES (blockSize=16), this is equivalent to PKCS5Padding.
func pkcs7Pad(data []byte, blockSize int) []byte {
	padLen := blockSize - (len(data) % blockSize)
	if padLen == 0 {
		padLen = blockSize
	}
	padding := make([]byte, padLen)
	for i := range padding {
		padding[i] = byte(padLen)
	}
	return append(data, padding...)
}

// pkcs7Unpad removes PKCS7 padding.
func pkcs7Unpad(data []byte, blockSize int) ([]byte, error) {
	if len(data) == 0 || len(data)%blockSize != 0 {
		return nil, fmt.Errorf("invalid padded plaintext length")
	}
	padLen := int(data[len(data)-1])
	if padLen <= 0 || padLen > blockSize {
		return nil, fmt.Errorf("invalid padding")
	}
	if padLen > len(data) {
		return nil, fmt.Errorf("invalid padding")
	}
	for i := 0; i < padLen; i++ {
		if data[len(data)-1-i] != byte(padLen) {
			return nil, fmt.Errorf("invalid padding")
		}
	}
	return data[:len(data)-padLen], nil
}

// LOCAL_KEY_CACHE is the hardcoded encryption key (same as DBeaver)
var LOCAL_KEY_CACHE = []byte{
	186, 187, 74, 159, 119, 74, 184, 83, 201, 108, 45, 101, 61, 254, 84, 74,
}

// CIPHER_NAME is the cipher algorithm used
const CIPHER_NAME = "AES/CBC/PKCS5Padding"

// Credentials represents the credentials.json structure
type Credentials struct {
	Version     int                        `json:"version"`
	Credentials map[string]CredentialEntry `json:"credentials"`
}

// CredentialEntry represents a single credential entry
type CredentialEntry struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoadCredentials loads credentials from ~/.swissql/credentials.json
func LoadCredentials() (*Credentials, error) {
	configDir, err := GetConfigDir()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(configDir, "credentials.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Return empty credentials if file doesn't exist
			return &Credentials{
				Version:     1,
				Credentials: make(map[string]CredentialEntry),
			}, nil
		}
		return nil, err
	}

	// Decrypt the data
	decrypted, err := DecryptCredentials(data)
	if err != nil {
		return nil, err
	}

	var credentials Credentials
	if err := json.Unmarshal(decrypted, &credentials); err != nil {
		return nil, err
	}

	return &credentials, nil
}

// SaveCredentials saves credentials to ~/.swissql/credentials.json
func SaveCredentials(credentials *Credentials) error {
	configDir, err := GetConfigDir()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(configDir, 0755); err != nil {
		return err
	}

	// Serialize to JSON
	data, err := json.Marshal(credentials)
	if err != nil {
		return err
	}

	// Encrypt the data
	encrypted, err := EncryptCredentials(data)
	if err != nil {
		return err
	}

	path := filepath.Join(configDir, "credentials.json")
	return os.WriteFile(path, encrypted, 0600)
}

// EncryptCredentials encrypts data using AES/CBC/PKCS5Padding
func EncryptCredentials(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(LOCAL_KEY_CACHE)
	if err != nil {
		return nil, err
	}

	plaintext := pkcs7Pad(data, aes.BlockSize)

	// Create IV
	ciphertext := make([]byte, aes.BlockSize+len(plaintext))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	// Encrypt
	stream := cipher.NewCBCEncrypter(block, iv)
	stream.CryptBlocks(ciphertext[aes.BlockSize:], plaintext)

	return ciphertext, nil
}

// DecryptCredentials decrypts data using AES/CBC/PKCS5Padding
func DecryptCredentials(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(LOCAL_KEY_CACHE)
	if err != nil {
		return nil, err
	}

	if len(data) < aes.BlockSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	iv := data[:aes.BlockSize]
	ciphertext := data[aes.BlockSize:]
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("ciphertext is not a multiple of the block size")
	}

	// Decrypt
	stream := cipher.NewCBCDecrypter(block, iv)
	stream.CryptBlocks(ciphertext, ciphertext)

	return pkcs7Unpad(ciphertext, aes.BlockSize)
}

// GetCredentials retrieves credentials for a profile ID
func GetCredentials(profileID string) (string, string, error) {
	credentials, err := LoadCredentials()
	if err != nil {
		return "", "", err
	}

	entry, exists := credentials.Credentials[profileID]
	if !exists {
		return "", "", nil
	}

	return entry.Username, entry.Password, nil
}

// SetCredentials saves credentials for a profile ID
func SetCredentials(profileID, username, password string) error {
	credentials, err := LoadCredentials()
	if err != nil {
		return err
	}

	credentials.Credentials[profileID] = CredentialEntry{
		Username: username,
		Password: password,
	}

	return SaveCredentials(credentials)
}
