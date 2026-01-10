package cmd

import (
	"fmt"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

const supportedSamplerTop = "top"

func supportedSamplersList() string {
	return supportedSamplerTop
}

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
		fmt.Printf("Error: missing sampler. Supported samplers: %s\n", supportedSamplersList())
		fmt.Printf("Usage: \\sampler <start|stop|restart|status> <sampler>\n")
		return true
	}

	action := strings.ToLower(strings.TrimSpace(args[0]))
	sampler := strings.ToLower(strings.TrimSpace(args[1]))

	if sampler != supportedSamplerTop {
		fmt.Printf("Error: unsupported sampler: %s. Supported samplers: %s\n", sampler, supportedSamplersList())
		return true
	}

	switch action {
	case "start":
		resp, err := c.TopSamplerStart(sessionId)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		printTopSamplerControlResponse(resp)
		return true
	case "stop":
		resp, err := c.TopSamplerStop(sessionId)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		printTopSamplerControlResponse(resp)
		return true
	case "restart":
		resp, err := c.TopSamplerRestart(sessionId)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		printTopSamplerControlResponse(resp)
		return true
	case "status":
		resp, err := c.TopSamplerStatus(sessionId)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		printTopSamplerStatusResponse(resp)
		return true
	default:
		fmt.Printf("Error: unsupported action: %s. Supported actions: %s\n", action, supportedSamplerActionsList())
		return true
	}
}

func printTopSamplerControlResponse(resp *client.TopSamplerControlResponse) {
	if resp == nil {
		fmt.Println("Error: empty response")
		return
	}
	if strings.TrimSpace(resp.Reason) != "" {
		fmt.Printf("%s (status=%s, reason=%s)\n", resp.Message, resp.Status, resp.Reason)
		return
	}
	fmt.Printf("%s (status=%s)\n", resp.Message, resp.Status)
}

func printTopSamplerStatusResponse(resp *client.TopSamplerStatusResponse) {
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
