package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Define the structure for the original document
type OriginalBot struct {
	OwnerID     string      `bson:"ownerID"`
	OwnerName   string      `bson:"ownerName"`
	BotID       string      `bson:"botID"`
	AltBotID    string      `bson:"BotID"` // Handle alternative field name which id BotID
	Username    string      `bson:"username"`
	Discrim     string      `bson:"discrim"`
	Avatar      string      `bson:"avatar"`
	Prefix      string      `bson:"prefix"`
	Invite      string      `bson:"invite"`
	LongDesc    string      `bson:"longDesc"`
	ShortDesc   string      `bson:"shortDesc"`
	Tags        []string    `bson:"tags"`
	Uptimerate  int         `bson:"uptimerate"`
	Coowners    []string    `bson:"coowners"`
	Premium     string      `bson:"premium"`
	Status      string      `bson:"status"`
	Website     string      `bson:"website"`
	Github      string      `bson:"github"`
	Support     string      `bson:"support"`
	Certificate string      `bson:"certificate"`
	Votes       interface{} `bson:"votes"`
	Token       string      `bson:"token"`
}

// Define the structure for the transformed document
type Bots struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Discriminator string   `json:"discriminator"`
	Website       string   `json:"website"`
	Github        string   `json:"github"`
	Avatar        string   `json:"avatar"`
	Tags          []string `json:"tags"`
	Votes         int      `json:"votes"`
	Reviews       []string `bson:"reviews"`
	Shortdesc     string   `json:"shortdesc"`
	Staff         string   `json:"staff"`
	Prefix        string   `json:"prefix"`
	Longdesc      string   `json:"longdesc"`
	Token         string   `json:"token"`
	Support       string   `json:"support"`
	OwnerAvatar   string   `json:"ownerAvatar"`
	OwnerName     string   `json:"ownerName"`
	Analytics     string   `json:"analytics"`
	Publicity     string   `json:"publicity"`
	Featured      bool     `bool:"featured"`
	Approved      bool     `bool:"approved"`
	Reviewing     bool     `bool:"reviewing"`
}

func main() {
	// Set up MongoDB connection
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb+srv://topiclist:topiclist@cluster0.uafa9.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"))
	if err != nil {
		log.Fatalf("Error creating MongoDB client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Log successful connection
	log.Println("Connected to MongoDB")

	// Access a database and a collection
	db := client.Database("myFirstDatabase")
	originalCollection := db.Collection("bots")
	transformedCollection := db.Collection("transformedbots")

	// Define a filter to get all documents from the original collection
	filter := bson.D{}

	// Retrieve documents from the original collection
	cur, err := originalCollection.Find(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close(ctx)

	// Initialize counter for processed documents
	processedDocs := 0

	// Set up channels for error logging
	errCh := make(chan error, 100) // Buffered channel for errors
	doneCh := make(chan struct{})  // Channel to signal completion

	// Set up WaitGroup for synchronizing goroutines
	var wg sync.WaitGroup

	// Measure start time
	startTime := time.Now()

	// Process documents concurrently
	for cur.Next(ctx) {
		var originalDoc OriginalBot
		err := cur.Decode(&originalDoc)
		if err != nil {
			errCh <- fmt.Errorf("error decoding document: %v", err)
			continue // Skip to the next document if there's an error decoding
		}

		// Increment WaitGroup counter
		wg.Add(1)

		// Process document in a goroutine
		go func(originalDoc OriginalBot) {
			defer wg.Done()

			// Transform the document to the desired structure
			transformedDoc, err := transformDocument(originalDoc)
			if err != nil {
				errCh <- fmt.Errorf("error transforming document: %v", err)
				log.Printf("Failed document details: %+v\n", originalDoc)
				return // Skip to the next document if there's an error transforming
			}

			// Insert the transformed document into the new collection
			_, err = transformedCollection.InsertOne(ctx, transformedDoc)
			if err != nil {
				errCh <- fmt.Errorf("error inserting document: %v", err)
				log.Printf("Failed document details: %+v\n", transformedDoc)
				return // Skip to the next document if there's an error inserting
			}

			// Increment processed document counter
			processedDocs++
		}(originalDoc)
	}

	// Close error channel when all goroutines are done
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// Listen for errors and log them
	go func() {
		for err := range errCh {
			log.Println(err)
		}
		doneCh <- struct{}{} // Signal completion
	}()

	// Wait for completion
	<-doneCh

	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	// Measure end time
	endTime := time.Now()

	// Calculate time taken
	elapsed := endTime.Sub(startTime)

	log.Printf("Conversion done. Processed %d documents in %v seconds.", processedDocs, elapsed.Seconds())
}

// Function to transform the document to the desired structure
func transformDocument(doc OriginalBot) (Bots, error) {
	// Check if essential fields are empty
	if doc.BotID == "" && doc.AltBotID == "" || doc.Username == "" || doc.Discrim == "" {
		return Bots{}, fmt.Errorf("essential fields are empty for document with BotID: %s", doc.BotID)
	}

	// Use BotID if available, otherwise use AltBotID
	id := doc.BotID
	if id == "" {
		id = doc.AltBotID
	}

	// Perform necessary transformations here
	var votes int
	switch v := doc.Votes.(type) {
	case int:
		votes = v
	case []interface{}:
		if len(v) > 0 {
			if num, ok := v[0].(int); ok {
				votes = num
			}
		}
	}

	return Bots{
		ID:            id,
		Name:          doc.Username,
		Discriminator: doc.Discrim,
		Website:       doc.Website,
		Github:        doc.Github,
		Avatar:        doc.Avatar,
		Tags:          doc.Tags,
		Votes:         votes,
		Reviews:       []string{}, // Empty for now, add logic if needed
		Shortdesc:     doc.ShortDesc,
		Prefix:        doc.Prefix,
		Longdesc:      doc.LongDesc,
		Support:       doc.Support,
		OwnerAvatar:   "", // Missing field in OriginalBot
		OwnerName:     doc.OwnerName,
		Token:         "",
		Analytics:     "",       // Missing field in OriginalBot
		Publicity:     "public", // Missing field in OriginalBot
		Approved:      true,
		Reviewing:     false,
		Featured:      false,
	}, nil
}
