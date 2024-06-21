package db

import (
	"log"
	"os"
	"os/exec"

	"github.com/joho/godotenv"
	"github.com/urfave/cli"
)

func db() {
	// Load environment variables
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	// Create a new CLI app
	app := cli.NewApp()
	app.Name = "Cli-Tools"
	app.Usage = "A simple library of cli tools built and used by topic to make the devs life easier!"
	app.Version = "1.0.0"

	// Define commands
	app.Commands = []cli.Command{
		{
			Name:    "Convert Struct",
			Aliases: []string{"conv"},
			Usage:   "Convert bot structure present in the old db for topic for the new db",
			Action: func(c *cli.Context) error {
				folderPath := "./mongo/botstructconv"
				cmd := exec.Command("go", "run", folderPath+".go")
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				return cmd.Run()
			},
		},
	}

	// Run the CLI app
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
