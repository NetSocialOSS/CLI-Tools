package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Define a map to hold available services and their corresponding directories
var services = map[string]string{
	"mongo": "./cli-tools/mongodb",
	"api":   "./cli-tools/API",
	// Add more services here as needed
}

func main() {
	// Parse command line flags
	var service string
	flag.StringVar(&service, "service", "", "Specify the service to run")
	flag.Parse()

	// Check if a service is specified
	if service == "" {
		fmt.Println("Usage: main -service=<service>")
		fmt.Println("Available services:")
		for svc := range services {
			fmt.Println("  -", svc)
		}
		os.Exit(1)
	}

	// Check if the specified service exists
	serviceDir, ok := services[service]
	if !ok {
		fmt.Println("Invalid service specified")
		os.Exit(1)
	}

	// Run the selected service
	err := runService(serviceDir)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

func runService(serviceDir string) error {
	// List files in the specified directory
	files, err := os.ReadDir(serviceDir)
	if err != nil {
		return err
	}

	// Print available tools in the service directory
	fmt.Println("Available tools in", serviceDir+":")
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".go" {
			fmt.Println("  -", file.Name())
		}
	}

	// Prompt user to select a tool
	fmt.Print("Enter the name of the tool you want to run: ")
	var tool string
	fmt.Scanln(&tool)

	// Construct the path to the tool executable
	toolPath := filepath.Join(serviceDir, tool)

	// Check if the tool executable exists
	_, err = os.Stat(toolPath)
	if os.IsNotExist(err) {
		return fmt.Errorf("tool %s not found", tool)
	}

	// Run the tool
	fmt.Println("Running", tool+"...")
	err = runCommand("go", "run", toolPath)
	if err != nil {
		return err
	}

	return nil
}

func runCommand(command string, args ...string) error {
	// Execute the command
	cmd := exec.Command(command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
