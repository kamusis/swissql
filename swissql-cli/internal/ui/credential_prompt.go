package ui

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

// PromptUsername prompts the user for a username
func PromptUsername() (string, error) {
	fmt.Print("Username: ")
	reader := bufio.NewReader(os.Stdin)
	username, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(username), nil
}

// PromptPassword prompts the user for a password (hidden input)
func PromptPassword() (string, error) {
	fmt.Print("Password: ")
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		return "", err
	}
	return string(password), nil
}

// PromptSavePassword prompts the user whether to save the password
func PromptSavePassword() (bool, error) {
	fmt.Print("Save password for future use? [y/N]: ")
	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(strings.ToLower(response)) == "y", nil
}
