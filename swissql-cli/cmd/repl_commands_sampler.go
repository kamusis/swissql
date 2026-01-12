package cmd

import (
	"fmt"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

func supportedSamplerActionsList() string {
	return "start, stop, restart, status"
}

// handleReplSamplerCommands handles sampler control commands such as:
// \sampler start top
// \sampler stop top
// \sampler restart top
// \sampler status top
func handleReplSamplerCommands(
	_ *cobra.Command,
	line *liner.State,
	historyMode string,
	input string,
	cmdName string,
	args []string,
	c *client.Client,
	sessionId string,
	_ *config.Config,
) bool {
	cmdLower := strings.ToLower(cmdName)
	if cmdLower != "\\sampler" {
		return false
	}

	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	if len(args) < 2 {
		fmt.Printf("Error: missing sampler. Usage: \\sampler <start|stop|restart|status> <sampler_id>\n")
		fmt.Printf("Usage: \\sampler <start|stop|restart|status> <sampler>\n")
		return true
	}

	action := strings.ToLower(strings.TrimSpace(args[0]))
	sampler := strings.ToLower(strings.TrimSpace(args[1]))

	switch action {
	case "start":
		enabled := true
		def := &client.SamplerDefinition{
			Enabled: &enabled,
		}
		resp, err := c.SamplerUpsert(sessionId, sampler, def)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		printSamplerControlResponse(resp)
		return true
	case "stop":
		resp, err := c.SamplerDelete(sessionId, sampler)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		printSamplerControlResponse(resp)
		return true
	case "restart":
		_, err := c.SamplerDelete(sessionId, sampler)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		enabled := true
		def := &client.SamplerDefinition{Enabled: &enabled}
		resp, err := c.SamplerUpsert(sessionId, sampler, def)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		printSamplerControlResponse(resp)
		return true
	case "status":
		resp, err := c.SamplerStatus(sessionId, sampler)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		printSamplerStatusResponse(resp)
		return true
	default:
		fmt.Printf("Error: unsupported action: %s. Supported actions: %s\n", action, supportedSamplerActionsList())
		return true
	}
}

func printSamplerControlResponse(resp *client.SamplerControlResponse) {
	if resp == nil {
		fmt.Println("Error: empty response")
		return
	}
	if strings.TrimSpace(resp.Reason) != "" {
		fmt.Printf("%s (status=%s, reason=%s)\n", resp.SamplerId, resp.Status, resp.Reason)
		return
	}
	fmt.Printf("%s (status=%s)\n", resp.SamplerId, resp.Status)
}

func printSamplerStatusResponse(resp *client.SamplerStatusResponse) {
	if resp == nil {
		fmt.Println("Error: empty response")
		return
	}
	if strings.TrimSpace(resp.Reason) != "" {
		fmt.Printf("%s (reason=%s)\n", resp.Status, resp.Reason)
		return
	}
	fmt.Printf("%s\n", resp.Status)
}
